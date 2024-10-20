package com.github.ares.spark.connector.source.partition.micro;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SupportCoordinate;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.spark.connector.source.partition.batch.ParallelBatchPartitionReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class AresMicroBatchPartitionReaderFactory implements PartitionReaderFactory {

    private final AresSource<AresRow, ?, ?> source;

    private final int parallelism;

    private final String checkpointLocation;

    private final CaseInsensitiveStringMap caseInsensitiveStringMap;

    public AresMicroBatchPartitionReaderFactory(
            AresSource<AresRow, ?, ?> source,
            int parallelism,
            String checkpointLocation,
            CaseInsensitiveStringMap caseInsensitiveStringMap) {
        this.source = source;
        this.parallelism = parallelism;
        this.checkpointLocation = checkpointLocation;
        this.caseInsensitiveStringMap = caseInsensitiveStringMap;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        AresMicroBatchInputPartition aresPartition =
                (AresMicroBatchInputPartition) partition;
        ParallelBatchPartitionReader partitionReader;
        Integer subtaskId = aresPartition.getSubtaskId();
        Integer checkpointId = aresPartition.getCheckpointId();
        Integer checkpointInterval = aresPartition.getCheckpointInterval();
        String hdfsRoot = aresPartition.getHdfsRoot();
        String hdfsUser = aresPartition.getHdfsUser();
        Map<String, String> envOptions = caseInsensitiveStringMap.asCaseSensitiveMap();
        if (source instanceof SupportCoordinate) {
            partitionReader =
                    new CoordinatedMicroBatchPartitionReader(
                            source,
                            parallelism,
                            subtaskId,
                            checkpointId,
                            checkpointInterval,
                            checkpointLocation,
                            hdfsRoot,
                            hdfsUser,
                            envOptions);
        } else {
            partitionReader =
                    new ParallelMicroBatchPartitionReader(
                            source,
                            parallelism,
                            subtaskId,
                            checkpointId,
                            checkpointInterval,
                            checkpointLocation,
                            hdfsRoot,
                            hdfsUser,
                            envOptions);
        }
        return new AresMicroBatchPartitionReader(partitionReader);
    }
}
