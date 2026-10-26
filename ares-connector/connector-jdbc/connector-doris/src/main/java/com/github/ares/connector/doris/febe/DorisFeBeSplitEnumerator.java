package com.github.ares.connector.doris.febe;

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

public class DorisFeBeSplitEnumerator
        implements SourceSplitEnumerator<DorisSplit, DorisSourceState> {
    private static final Logger log = LoggerFactory.getLogger(DorisFeBeSplitEnumerator.class);

    private final Context<DorisSplit> context;
    private final DorisFeBeConfig config;
    private final Set<String> assignedSplitIds;
    private final Object lock = new Object();

    public DorisFeBeSplitEnumerator(
            Context<DorisSplit> context, DorisFeBeConfig config, Set<String> assignedSplitIds) {
        this.context = context;
        this.config = config;
        this.assignedSplitIds = new HashSet<>(assignedSplitIds);
    }

    @Override
    public void open() {}

    @Override
    public void run() {
        List<DorisSplit> splits = QueryPlanClient.planSplits(config);
        Map<Integer, List<DorisSplit>> assignment = new LinkedHashMap<>();
        int parallelism = Math.max(context.currentParallelism(), 1);
        for (DorisSplit split : splits) {
            if (assignedSplitIds.contains(split.splitId())) {
                continue;
            }
            int subtask = Math.floorMod(assignmentSize(assignment), parallelism);
            assignment.computeIfAbsent(subtask, key -> new ArrayList<>()).add(split);
            assignedSplitIds.add(split.splitId());
        }
        log.info(
                "Doris febe assigned {} splits across {} readers",
                assignedSplitIds.size(),
                parallelism);
        for (int subtask = 0; subtask < parallelism; subtask++) {
            List<DorisSplit> assigned = assignment.get(subtask);
            if (assigned != null && !assigned.isEmpty()) {
                context.assignSplit(subtask, assigned);
            }
            context.signalNoMoreSplits(subtask);
        }
    }

    private static int assignmentSize(Map<Integer, List<DorisSplit>> assignment) {
        int size = 0;
        for (List<DorisSplit> splits : assignment.values()) {
            size += splits.size();
        }
        return size;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<DorisSplit> splits, int subtaskId) {
        synchronized (lock) {
            for (DorisSplit split : splits) {
                assignedSplitIds.remove(split.splitId());
            }
        }
        context.assignSplit(subtaskId, splits);
        for (DorisSplit split : splits) {
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
    public DorisSourceState snapshotState(long checkpointId) {
        synchronized (lock) {
            return new DorisSourceState(assignedSplitIds);
        }
    }
}
