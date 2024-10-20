package com.github.ares.connector.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.Collector;
import com.github.ares.api.source.SourceEvent;
import com.github.ares.api.source.SourceReader;
import com.github.ares.api.source.SourceSplit;
import com.github.ares.api.source.SourceSplitEnumerator;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.serialization.Serializer;
import com.github.ares.connector.utils.ThreadPoolExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CoordinatedSource<T, SplitT extends SourceSplit, StateT extends Serializable>
        implements BaseSourceFunction<T> {
    private static final Logger log = LoggerFactory.getLogger(CoordinatedSource.class);

    protected static final long SLEEP_TIME_INTERVAL = 5L;
    protected final AresSource<T, SplitT, StateT> source;
    protected final Map<Integer, List<byte[]>> restoredState;
    protected final Integer parallelism;

    protected final Serializer<SplitT> splitSerializer;
    protected final Serializer<StateT> enumeratorStateSerializer;

    protected final CoordinatedEnumeratorContext<SplitT> coordinatedEnumeratorContext;
    protected final Map<Integer, CoordinatedReaderContext> readerContextMap;
    protected final Map<Integer, List<SplitT>> restoredSplitStateMap = new HashMap<>();

    protected transient volatile SourceSplitEnumerator<SplitT, StateT> splitEnumerator;
    protected transient Map<Integer, SourceReader<T, SplitT>> readerMap = new ConcurrentHashMap<>();
    protected final Map<Integer, AtomicBoolean> readerRunningMap;
    protected final AtomicInteger completedReader = new AtomicInteger(0);
    protected transient volatile ScheduledThreadPoolExecutor executorService;

    /**
     * Flag indicating whether the consumer is still running.
     */
    protected volatile boolean running = true;

    public CoordinatedSource(
            AresSource<T, SplitT, StateT> source,
            Map<Integer, List<byte[]>> restoredState,
            int parallelism) {
        this.source = source;
        this.restoredState = restoredState;
        this.parallelism = parallelism;
        this.splitSerializer = source.getSplitSerializer();
        this.enumeratorStateSerializer = source.getEnumeratorStateSerializer();

        this.coordinatedEnumeratorContext = new CoordinatedEnumeratorContext<>(this);
        this.readerContextMap = new ConcurrentHashMap<>(parallelism);
        this.readerRunningMap = new ConcurrentHashMap<>(parallelism);
        try {
            createSplitEnumerator();
            createReaders();
        } catch (Exception e) {
            log.warn("create split enumerator or readers failed", e);
        }
    }

    private void createSplitEnumerator() throws Exception {
        if (restoredState != null && !restoredState.isEmpty()) {
            StateT restoredEnumeratorState = null;
            if (restoredState.containsKey(-1)) {
                restoredEnumeratorState =
                        enumeratorStateSerializer.deserialize(restoredState.get(-1).get(0));
            }
            splitEnumerator =
                    source.restoreEnumerator(coordinatedEnumeratorContext, restoredEnumeratorState);
            restoredState.forEach(
                    (subtaskId, splitBytes) -> {
                        if (subtaskId == -1) {
                            return;
                        }
                        List<SplitT> restoredSplitState = new ArrayList<>(splitBytes.size());
                        for (byte[] splitByte : splitBytes) {
                            try {
                                restoredSplitState.add(splitSerializer.deserialize(splitByte));
                            } catch (IOException e) {
                                throw new AresException(e);
                            }
                        }
                        restoredSplitStateMap.put(subtaskId, restoredSplitState);
                    });
        } else {
            splitEnumerator = source.createEnumerator(coordinatedEnumeratorContext);
        }
    }

    private void createReaders() throws Exception {
        for (int subtaskId = 0; subtaskId < this.parallelism; subtaskId++) {
            CoordinatedReaderContext readerContext =
                    new CoordinatedReaderContext(this, source.getBoundedness(), subtaskId);
            readerContextMap.put(subtaskId, readerContext);
            readerRunningMap.put(subtaskId, new AtomicBoolean(true));
            SourceReader<T, SplitT> reader = source.createReader(readerContext);
            readerMap.put(subtaskId, reader);
        }
    }

    @Override
    public void open() throws Exception {
        executorService =
                ThreadPoolExecutorFactory.createScheduledThreadPoolExecutor(
                        parallelism, "parallel-split-enumerator-executor");
        splitEnumerator.open();
        restoredSplitStateMap.forEach(
                (subtaskId, splits) -> splitEnumerator.addSplitsBack(splits, subtaskId));
        readerMap
                .entrySet()
                .parallelStream()
                .forEach(
                        entry -> {
                            try {
                                entry.getValue().open();
                                splitEnumerator.registerReader(entry.getKey());
                            } catch (Exception e) {
                                throw new AresException(e);
                            }
                        });
    }

    @Override
    public void run(Collector<T> collector) throws Exception {
        readerMap
                .entrySet()
                .parallelStream()
                .forEach(
                        entry -> {
                            final AtomicBoolean flag = readerRunningMap.get(entry.getKey());
                            final SourceReader<T, SplitT> reader = entry.getValue();
                            executorService.execute(
                                    () -> {
                                        while (flag.get()) {
                                            try {
                                                reader.pollNext(collector);
                                                if (collector.isEmptyThisPollNext()) {
                                                    Thread.sleep(100);
                                                } else {
                                                    collector.resetEmptyThisPollNext();
                                                    Thread.sleep(0L);
                                                }
                                            } catch (Exception e) {
                                                running = false;
                                                flag.set(false);
                                                throw new AresException(e);
                                            }
                                        }
                                    });
                        });
        splitEnumerator.run();
        while (running) {
            Thread.sleep(SLEEP_TIME_INTERVAL);
        }
    }

    @Override
    public void close() throws IOException {
        running = false;

        for (Map.Entry<Integer, SourceReader<T, SplitT>> entry : readerMap.entrySet()) {
            readerRunningMap.get(entry.getKey()).set(false);
            entry.getValue().close();
        }

        if (executorService != null) {
            executorService.shutdown();
        }

        try (SourceSplitEnumerator<SplitT, StateT> closed = splitEnumerator) {
            // just close the resources
        }
    }

    // --------------------------------------------------------------------------------------------
    // Checkpoint & state
    // --------------------------------------------------------------------------------------------

    @Override
    public Map<Integer, List<byte[]>> snapshotState(long checkpointId) throws Exception {
        Map<Integer, List<byte[]>> allStates =
                readerMap
                        .entrySet()
                        .parallelStream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry<Integer, SourceReader<T, SplitT>>::getKey,
                                        readerEntry -> {
                                            try {
                                                List<SplitT> splitStates =
                                                        readerEntry
                                                                .getValue()
                                                                .snapshotState(checkpointId);
                                                final List<byte[]> rawValues =
                                                        new ArrayList<>(splitStates.size());
                                                for (SplitT splitState : splitStates) {
                                                    rawValues.add(
                                                            splitSerializer.serialize(splitState));
                                                }
                                                return rawValues;
                                            } catch (Exception e) {
                                                throw new AresException(e);
                                            }
                                        }));
        StateT enumeratorState = splitEnumerator.snapshotState(checkpointId);
        if (enumeratorState != null) {
            byte[] enumeratorStateBytes = enumeratorStateSerializer.serialize(enumeratorState);
            allStates.put(-1, Collections.singletonList(enumeratorStateBytes));
        }
        return allStates;
    }

    // --------------------------------------------------------------------------------------------
    // Reader context methods
    // --------------------------------------------------------------------------------------------

    protected void handleNoMoreElement(int subtaskId) {
        readerRunningMap.get(subtaskId).set(false);
        readerContextMap.remove(subtaskId);
        if (completedReader.incrementAndGet() == this.parallelism) {
            this.running = false;
        }
    }

    protected void handleSplitRequest(int subtaskId) {
        splitEnumerator.handleSplitRequest(subtaskId);
    }

    protected void handleReaderEvent(int subtaskId, SourceEvent event) {
        splitEnumerator.handleSourceEvent(subtaskId, event);
    }

    // --------------------------------------------------------------------------------------------
    // Enumerator context methods
    // --------------------------------------------------------------------------------------------

    public int currentReaderCount() {
        return readerContextMap.size();
    }

    public Set<Integer> registeredReaders() {
        return readerMap.keySet();
    }

    protected void addSplits(int subtaskId, List<SplitT> splits) {
        readerMap.get(subtaskId).addSplits(splits);
    }

    protected void handleNoMoreSplits(int subtaskId) {
        readerMap.get(subtaskId).handleNoMoreSplits();
    }

    protected void handleEnumeratorEvent(int subtaskId, SourceEvent event) {
        readerMap.get(subtaskId).handleSourceEvent(event);
    }
}
