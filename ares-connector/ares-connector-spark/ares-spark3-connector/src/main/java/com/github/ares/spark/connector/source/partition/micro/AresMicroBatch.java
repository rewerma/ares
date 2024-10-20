package com.github.ares.spark.connector.source.partition.micro;

import com.github.ares.api.env.EnvCommonOptions;
import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SupportCoordinate;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.utils.Constants;
import com.github.ares.common.utils.JsonUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.ArrayList;
import java.util.List;

public class AresMicroBatch implements MicroBatchStream {

    public static final Integer CHECKPOINT_INTERVAL_DEFAULT = 10000;

    private final AresSource<AresRow, ?, ?> source;

    private final int parallelism;

    private final String checkpointLocation;

    private final CaseInsensitiveStringMap caseInsensitiveStringMap;

    private final Offset initialOffset = AresOffset.of(0L);

    private Offset currentOffset = initialOffset;

    public AresMicroBatch(
            AresSource<AresRow, ?, ?> source,
            int parallelism,
            String checkpointLocation,
            CaseInsensitiveStringMap caseInsensitiveStringMap) {
        this.source = source;
        this.parallelism = parallelism;
        this.checkpointLocation = checkpointLocation;
        this.caseInsensitiveStringMap = caseInsensitiveStringMap;
    }

    public AresSource<AresRow, ?, ?> getSource() {
        return source;
    }

    public int getParallelism() {
        return parallelism;
    }

    public String getCheckpointLocation() {
        return checkpointLocation;
    }

    public CaseInsensitiveStringMap getCaseInsensitiveStringMap() {
        return caseInsensitiveStringMap;
    }

    public Offset getInitialOffset() {
        return initialOffset;
    }

    public Offset getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public Offset latestOffset() {
        return currentOffset;
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        int checkpointInterval =
                caseInsensitiveStringMap.getInt(
                        EnvCommonOptions.CHECKPOINT_INTERVAL.key(), CHECKPOINT_INTERVAL_DEFAULT);
        Configuration configuration =
                SparkSession.getActiveSession().get().sparkContext().hadoopConfiguration();
        String hdfsRoot =
                caseInsensitiveStringMap.getOrDefault(
                        Constants.HDFS_ROOT, FileSystem.getDefaultUri(configuration).toString());
        String hdfsUser = caseInsensitiveStringMap.getOrDefault(Constants.HDFS_USER, "");
        List<InputPartition> virtualPartitions;
        if (source instanceof SupportCoordinate) {
            virtualPartitions = new ArrayList<>(1);
            virtualPartitions.add(
                    new AresMicroBatchInputPartition(
                            source,
                            parallelism,
                            0,
                            1,
                            checkpointInterval,
                            checkpointLocation,
                            hdfsRoot,
                            hdfsUser));
        } else {
            virtualPartitions = new ArrayList<>(parallelism);
            for (int subtaskId = 0; subtaskId < parallelism; subtaskId++) {
                virtualPartitions.add(
                        new AresMicroBatchInputPartition(
                                source,
                                parallelism,
                                subtaskId,
                                1,
                                checkpointInterval,
                                checkpointLocation,
                                hdfsRoot,
                                hdfsUser));
            }
        }
        return virtualPartitions.toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new AresMicroBatchPartitionReaderFactory(
                source, parallelism, checkpointLocation, caseInsensitiveStringMap);
    }

    @Override
    public Offset initialOffset() {
        return initialOffset;
    }

    @Override
    public Offset deserializeOffset(String json) {
        return JsonUtils.parseObject(json, AresOffset.class);
    }

    @Override
    public void commit(Offset end) {
        this.currentOffset = ((AresOffset) end).inc();
    }

    @Override
    public void stop() {
        // do nothing
    }
}
