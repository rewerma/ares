package com.github.ares.connector.starrocks.febe;

import com.github.ares.api.source.SourceSplitEnumerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarRocksFeBeSplitEnumerator
        implements SourceSplitEnumerator<StarRocksSplit, StarRocksSourceState> {
    private static final Logger log = LoggerFactory.getLogger(StarRocksFeBeSplitEnumerator.class);

    private final Context<StarRocksSplit> context;
    private final StarRocksFeBeConfig config;
    private final Set<String> assignedSplitIds;
    private final Object lock = new Object();

    public StarRocksFeBeSplitEnumerator(
            Context<StarRocksSplit> context,
            StarRocksFeBeConfig config,
            Set<String> assignedSplitIds) {
        this.context = context;
        this.config = config;
        this.assignedSplitIds = new HashSet<>(assignedSplitIds);
    }

    @Override
    public void open() {}

    @Override
    public void run() {
        List<StarRocksSplit> splits = QueryPlanClient.planSplits(config);
        Map<Integer, List<StarRocksSplit>> assignment = new LinkedHashMap<>();
        int parallelism = Math.max(context.currentParallelism(), 1);
        for (StarRocksSplit split : splits) {
            if (assignedSplitIds.contains(split.splitId())) {
                continue;
            }
            int subtask = Math.floorMod(assignmentSize(assignment), parallelism);
            assignment.computeIfAbsent(subtask, key -> new ArrayList<>()).add(split);
            assignedSplitIds.add(split.splitId());
        }
        log.info(
                "StarRocks febe assigned {} splits across {} readers",
                assignedSplitIds.size(),
                parallelism);
        for (int subtask = 0; subtask < parallelism; subtask++) {
            List<StarRocksSplit> assigned = assignment.get(subtask);
            if (assigned != null && !assigned.isEmpty()) {
                context.assignSplit(subtask, assigned);
            }
            context.signalNoMoreSplits(subtask);
        }
    }

    private static int assignmentSize(Map<Integer, List<StarRocksSplit>> assignment) {
        int size = 0;
        for (List<StarRocksSplit> splits : assignment.values()) {
            size += splits.size();
        }
        return size;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<StarRocksSplit> splits, int subtaskId) {
        synchronized (lock) {
            for (StarRocksSplit split : splits) {
                assignedSplitIds.remove(split.splitId());
            }
        }
        context.assignSplit(subtaskId, splits);
        for (StarRocksSplit split : splits) {
            assignedSplitIds.add(split.splitId());
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {}

    @Override
    public StarRocksSourceState snapshotState(long checkpointId) {
        synchronized (lock) {
            return new StarRocksSourceState(assignedSplitIds);
        }
    }
}
