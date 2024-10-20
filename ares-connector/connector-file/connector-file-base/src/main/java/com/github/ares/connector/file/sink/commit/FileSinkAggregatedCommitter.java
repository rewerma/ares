package com.github.ares.connector.file.sink.commit;

import com.github.ares.api.sink.SinkAggregatedCommitter;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.hadoop.HadoopFileSystemProxy;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class FileSinkAggregatedCommitter
        implements SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo> {
    protected HadoopFileSystemProxy hadoopFileSystemProxy;

    public FileSinkAggregatedCommitter(HadoopConf hadoopConf) {
        this.hadoopFileSystemProxy = new HadoopFileSystemProxy(hadoopConf);
    }

    @Override
    public List<FileAggregatedCommitInfo> commit(
            List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        List<FileAggregatedCommitInfo> errorAggregatedCommitInfoList = new ArrayList<>();
        aggregatedCommitInfos.forEach(
                aggregatedCommitInfo -> {
                    try {
                        for (Map.Entry<String, LinkedHashMap<String, String>> entry :
                                aggregatedCommitInfo.getTransactionMap().entrySet()) {
                            for (Map.Entry<String, String> mvFileEntry :
                                    entry.getValue().entrySet()) {
                                // first rename temp file
                                hadoopFileSystemProxy.renameFile(
                                        mvFileEntry.getKey(), mvFileEntry.getValue(), true);
                            }
                            // second delete transaction directory
                            hadoopFileSystemProxy.deleteFile(entry.getKey());
                        }
                    } catch (Throwable e) {
                        log.error(
                                "commit aggregatedCommitInfo error, aggregatedCommitInfo = {} ",
                                aggregatedCommitInfo,
                                e);
                        errorAggregatedCommitInfoList.add(aggregatedCommitInfo);
                    }
                });
        return errorAggregatedCommitInfoList;
    }

    /**
     * The logic about how to combine commit message.
     *
     * @param commitInfos The list of commit message.
     * @return The commit message after combine.
     */
    @Override
    public FileAggregatedCommitInfo combine(List<FileCommitInfo> commitInfos) {
        if (commitInfos == null || commitInfos.size() == 0) {
            return null;
        }
        LinkedHashMap<String, LinkedHashMap<String, String>> aggregateCommitInfo =
                new LinkedHashMap<>();
        LinkedHashMap<String, List<String>> partitionDirAndValuesMap = new LinkedHashMap<>();
        commitInfos.forEach(
                commitInfo -> {
                    LinkedHashMap<String, String> needMoveFileMap =
                            aggregateCommitInfo.computeIfAbsent(
                                    commitInfo.getTransactionDir(), k -> new LinkedHashMap<>());
                    needMoveFileMap.putAll(commitInfo.getNeedMoveFiles());
                    if (commitInfo.getPartitionDirAndValuesMap() != null
                            && !commitInfo.getPartitionDirAndValuesMap().isEmpty()) {
                        partitionDirAndValuesMap.putAll(commitInfo.getPartitionDirAndValuesMap());
                    }
                });
        return new FileAggregatedCommitInfo(aggregateCommitInfo, partitionDirAndValuesMap);
    }

    /**
     * If {@link #commit(List)} failed, this method will be called (**Only** on Spark engine at
     * now).
     *
     * @param aggregatedCommitInfos The list of combine commit message.
     * @throws Exception throw Exception when abort failed.
     */
    @Override
    public void abort(List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws Exception {
        log.info("rollback aggregate commit");
        if (aggregatedCommitInfos == null || aggregatedCommitInfos.size() == 0) {
            return;
        }
        aggregatedCommitInfos.forEach(
                aggregatedCommitInfo -> {
                    try {
                        for (Map.Entry<String, LinkedHashMap<String, String>> entry :
                                aggregatedCommitInfo.getTransactionMap().entrySet()) {
                            // rollback the file
                            for (Map.Entry<String, String> mvFileEntry :
                                    entry.getValue().entrySet()) {
                                if (hadoopFileSystemProxy.fileExist(mvFileEntry.getValue())
                                        && !hadoopFileSystemProxy.fileExist(mvFileEntry.getKey())) {
                                    hadoopFileSystemProxy.renameFile(
                                            mvFileEntry.getValue(), mvFileEntry.getKey(), true);
                                }
                            }
                            // delete the transaction dir
                            hadoopFileSystemProxy.deleteFile(entry.getKey());
                        }
                    } catch (Exception e) {
                        log.error("abort aggregatedCommitInfo error ", e);
                    }
                });
    }

    /**
     * Close this resource.
     *
     * @throws IOException throw IOException when close failed.
     */
    @Override
    public void close() throws IOException {
        hadoopFileSystemProxy.close();
    }
}
