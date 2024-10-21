package com.github.ares.connector.hive.commit;

import com.github.ares.com.typesafe.config.Config;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.sink.commit.FileAggregatedCommitInfo;
import com.github.ares.connector.hive.utils.HiveMetaStoreProxy;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.ares.connector.file.sink.commit.FileSinkAggregatedCommitter;

import static com.github.ares.connector.hive.config.HiveConfig.ABORT_DROP_PARTITION_METADATA;

@Slf4j
public class HiveSinkAggregatedCommitter extends FileSinkAggregatedCommitter {
    private final Config pluginConfig;
    private final String dbName;
    private final String tableName;
    private final boolean abortDropPartitionMetadata;

    public HiveSinkAggregatedCommitter(
            Config pluginConfig, String dbName, String tableName, HadoopConf hadoopConf) {
        super(hadoopConf);
        this.pluginConfig = pluginConfig;
        this.dbName = dbName;
        this.tableName = tableName;
        this.abortDropPartitionMetadata =
                pluginConfig.hasPath(ABORT_DROP_PARTITION_METADATA.key())
                        ? pluginConfig.getBoolean(ABORT_DROP_PARTITION_METADATA.key())
                        : ABORT_DROP_PARTITION_METADATA.defaultValue();
    }

    @Override
    public List<FileAggregatedCommitInfo> commit(
            List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        HiveMetaStoreProxy hiveMetaStore = HiveMetaStoreProxy.getInstance(pluginConfig);
        List<FileAggregatedCommitInfo> errorCommitInfos = super.commit(aggregatedCommitInfos);
        if (errorCommitInfos.isEmpty()) {
            for (FileAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
                Map<String, List<String>> partitionDirAndValuesMap =
                        aggregatedCommitInfo.getPartitionDirAndValuesMap();
                List<String> partitions =
                        partitionDirAndValuesMap.keySet().stream()
                                .map(partition -> partition.replaceAll("\\\\", "/"))
                                .collect(Collectors.toList());
                try {
                    hiveMetaStore.addPartitions(dbName, tableName, partitions);
                    log.info("Add these partitions {}", partitions);
                } catch (TException e) {
                    log.error("Failed to add these partitions {}", partitions, e);
                    errorCommitInfos.add(aggregatedCommitInfo);
                }
            }
        }
        hiveMetaStore.close();
        return errorCommitInfos;
    }

    @Override
    public void abort(List<FileAggregatedCommitInfo> aggregatedCommitInfos) throws Exception {
        super.abort(aggregatedCommitInfos);
        if (abortDropPartitionMetadata) {
            HiveMetaStoreProxy hiveMetaStore = HiveMetaStoreProxy.getInstance(pluginConfig);
            for (FileAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
                Map<String, List<String>> partitionDirAndValuesMap =
                        aggregatedCommitInfo.getPartitionDirAndValuesMap();
                List<String> partitions =
                        partitionDirAndValuesMap.keySet().stream()
                                .map(partition -> partition.replaceAll("\\\\", "/"))
                                .collect(Collectors.toList());
                try {
                    hiveMetaStore.dropPartitions(dbName, tableName, partitions);
                    log.info("Remove these partitions {}", partitions);
                } catch (TException e) {
                    log.error("Failed to remove these partitions {}", partitions, e);
                }
            }
            hiveMetaStore.close();
        }
    }
}
