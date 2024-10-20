package com.github.ares.connector.file.source.split;

import com.github.ares.api.source.SourceSplitEnumerator;
import com.github.ares.connector.file.config.BaseMultipleTableFileSourceConfig;
import com.github.ares.connector.file.source.state.FileSourceState;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.github.ares.connector.file.config.BaseFileSourceConfig;


@Slf4j
public class MultipleTableFileSourceSplitEnumerator
        implements SourceSplitEnumerator<FileSourceSplit, FileSourceState> {

    private final Context<FileSourceSplit> context;
    private final Set<FileSourceSplit> pendingSplit;
    private final Set<FileSourceSplit> assignedSplit;
    private final Map<String, List<String>> filePathMap;

    public MultipleTableFileSourceSplitEnumerator(
            Context<FileSourceSplit> context,
            BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig) {
        this.context = context;
        this.filePathMap =
                multipleTableFileSourceConfig.getFileSourceConfigs().stream()
                        .collect(
                                Collectors.toMap(
                                        localFileSourceConfig ->
                                                localFileSourceConfig
                                                        .getCatalogTable()
                                                        .getTableId()
                                                        .toTablePath()
                                                        .toString(),
                                        BaseFileSourceConfig::refreshFilePaths));
        this.assignedSplit = new HashSet<>();
        this.pendingSplit = new HashSet<>();
    }

    public MultipleTableFileSourceSplitEnumerator(
            Context<FileSourceSplit> context,
            BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig,
            FileSourceState fileSourceState) {
        this(context, multipleTableFileSourceConfig);
        this.assignedSplit.addAll(fileSourceState.getAssignedSplit());
    }

    @Override
    public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
        if (CollectionUtils.isEmpty(splits)) {
            return;
        }
        pendingSplit.addAll(splits);
        assignSplit(subtaskId);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {
        for (Map.Entry<String, List<String>> filePathEntry : filePathMap.entrySet()) {
            String tableId = filePathEntry.getKey();
            List<String> filePaths = filePathEntry.getValue();
            for (String filePath : filePaths) {
                pendingSplit.add(new FileSourceSplit(tableId, filePath));
            }
        }
        assignSplit(subtaskId);
    }

    @Override
    public FileSourceState snapshotState(long checkpointId) {
        return new FileSourceState(assignedSplit);
    }

//    @Override
//    public void notifyCheckpointComplete(long checkpointId) {
//        // do nothing.
//    }

    private void assignSplit(int taskId) {
        List<FileSourceSplit> currentTaskSplits = new ArrayList<>();
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
        currentTaskSplits.forEach(pendingSplit::remove);
        log.info(
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
    public void open() {
        // do nothing
    }

    @Override
    public void run() throws Exception {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
