package com.github.ares.spark.connector.source.partition.batch;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.Collector;
import com.github.ares.api.source.SourceReader;
import com.github.ares.api.source.SourceSplit;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.connector.source.BaseSourceFunction;
import com.github.ares.connector.source.CoordinatedSource;
import com.github.ares.spark.connector.serialization.InternalRowCollector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CoordinatedBatchPartitionReader extends ParallelBatchPartitionReader {

    protected final Map<Integer, InternalRowCollector> collectorMap;

    public CoordinatedBatchPartitionReader(
            AresSource<AresRow, ?, ?> source,
            Integer parallelism,
            Integer subtaskId,
            Map<String, String> envOptions) {
        super(source, parallelism, subtaskId, envOptions);
        this.collectorMap = new HashMap<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            collectorMap.put(
                    i,
                    new InternalRowCollector(
                            handover, new Object(), source.getProducedType(), envOptions));
        }
    }

    @Override
    protected String getEnumeratorThreadName() {
        return "coordinated-split-enumerator-executor";
    }

    @Override
    protected BaseSourceFunction<AresRow> createInternalSource() {
        return new InternalCoordinatedSource<>(source, null, parallelism);
    }

    public class InternalCoordinatedSource<SplitT extends SourceSplit, StateT extends Serializable>
            extends CoordinatedSource<AresRow, SplitT, StateT> {

        public InternalCoordinatedSource(
                AresSource<AresRow, SplitT, StateT> source,
                Map<Integer, List<byte[]>> restoredState,
                int parallelism) {
            super(source, restoredState, parallelism);
        }

        @Override
        public void run(Collector<AresRow> collector) throws Exception {
            readerMap
                    .entrySet()
                    .parallelStream()
                    .forEach(
                            entry -> {
                                final AtomicBoolean flag = readerRunningMap.get(entry.getKey());
                                final SourceReader<AresRow, SplitT> reader = entry.getValue();
                                final Collector<AresRow> rowCollector =
                                        collectorMap.get(entry.getKey());
                                executorService.execute(
                                        () -> {
                                            while (flag.get()) {
                                                try {
                                                    reader.pollNext(rowCollector);
                                                    if (rowCollector.isEmptyThisPollNext()) {
                                                        Thread.sleep(100);
                                                    } else {
                                                        rowCollector.resetEmptyThisPollNext();
                                                        Thread.sleep(0L);
                                                    }
                                                } catch (Exception e) {
                                                    this.running = false;
                                                    flag.set(false);
                                                    throw new RuntimeException(e);
                                                }
                                            }
                                        });
                            });
            splitEnumerator.run();
            while (this.running) {
                Thread.sleep(SLEEP_TIME_INTERVAL);
            }
        }

        @Override
        protected void handleNoMoreElement(int subtaskId) {
            super.handleNoMoreElement(subtaskId);
            if (!this.running) {
                CoordinatedBatchPartitionReader.this.running = false;
            }
        }
    }
}
