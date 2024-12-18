package com.github.ares.connector.file.source.split;

import com.github.ares.api.source.SourceSplitEnumerator;
import com.github.ares.connector.file.source.state.FileSourceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class FileSourceSplitEnumerator
        implements SourceSplitEnumerator<FileSourceSplit, FileSourceState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSourceSplitEnumerator.class);

    private final Context<FileSourceSplit> context;
    private final Set<FileSourceSplit> pendingSplit = new HashSet<>();
    private Set<FileSourceSplit> assignedSplit;
    private final List<String> filePaths;

    public FileSourceSplitEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> context, List<String> filePaths) {
        this.context = context;
        this.filePaths = filePaths;
        this.assignedSplit = new HashSet<>();
    }

    public FileSourceSplitEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> context,
            List<String> filePaths,
            FileSourceState sourceState) {
        this(context, filePaths);
        this.assignedSplit = sourceState.getAssignedSplit();
    }

    @Override
    public void open() {
        this.pendingSplit.addAll(discoverySplits());
    }

    @Override
    public void run() {
        for (int i = 0; i < context.currentParallelism(); i++) {
            LOGGER.info("Assigned splits to reader [{}]", i);
            assignSplit(i);
        }
    }

    private Set<FileSourceSplit> discoverySplits() {
        Set<FileSourceSplit> fileSourceSplits = new HashSet<>();
        filePaths.forEach(k -> fileSourceSplits.add(new FileSourceSplit(k)));
        return fileSourceSplits;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            pendingSplit.addAll(splits);
            assignSplit(subtaskId);
        }
    }

    private void assignSplit(int taskId) {
        ArrayList<FileSourceSplit> currentTaskSplits = new ArrayList<>();
        if (context.currentParallelism() == 1) {
            // if parallelism == 1, we should assign all the splits to reader
            currentTaskSplits.addAll(pendingSplit);
        } else {
            // if parallelism > 1, according to hashCode of split's id to determine whether to
            // allocate the current task
            for (FileSourceSplit fileSourceSplit : pendingSplit) {
                int splitOwner =
                        getSplitOwner(fileSourceSplit.splitId(), context.currentParallelism());
                if (splitOwner == taskId) {
                    currentTaskSplits.add(fileSourceSplit);
                }
            }
        }
        // assign splits
        context.assignSplit(taskId, currentTaskSplits);
        // save the state of assigned splits
        assignedSplit.addAll(currentTaskSplits);
        // remove the assigned splits from pending splits
        currentTaskSplits.forEach(split -> pendingSplit.remove(split));
        LOGGER.info(
                "SubTask {} is assigned to [{}]",
                taskId,
                currentTaskSplits.stream()
                        .map(FileSourceSplit::splitId)
                        .collect(Collectors.joining(",")));
        context.signalNoMoreSplits(taskId);
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        // do nothing
    }

    @Override
    public FileSourceState snapshotState(long checkpointId) {
        return new FileSourceState(assignedSplit);
    }

//    @Override
//    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void handleSplitRequest(int subtaskId) {
    }
}
