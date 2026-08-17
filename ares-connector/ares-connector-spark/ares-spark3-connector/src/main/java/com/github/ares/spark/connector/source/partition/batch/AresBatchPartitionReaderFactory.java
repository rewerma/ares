package com.github.ares.spark.connector.source.partition.batch;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SupportCoordinate;
import com.github.ares.api.table.type.AresRow;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.util.LongAccumulator;

public class AresBatchPartitionReaderFactory implements PartitionReaderFactory {
    private final LongAccumulator readAccumulator;

    private final AresSource<AresRow, ?, ?> source;

    private final int parallelism;
    private final Map<String, String> envOptions;

    public AresBatchPartitionReaderFactory(
            AresSource<AresRow, ?, ?> source,
            int parallelism,
            Map<String, String> envOptions,
            LongAccumulator readAccumulator) {
        this.source = source;
        this.parallelism = parallelism;
        this.envOptions = envOptions;
        this.readAccumulator = readAccumulator;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        AresBatchInputPartition inputPartition = (AresBatchInputPartition) partition;
        int partitionId = inputPartition.getPartitionId();
        ParallelBatchPartitionReader partitionReader;
        if (source instanceof SupportCoordinate) {
            partitionReader =
                    new CoordinatedBatchPartitionReader(
                            source, parallelism, partitionId, envOptions);
        } else {
            partitionReader =
                    new ParallelBatchPartitionReader(source, parallelism, partitionId, envOptions);
        }
        return new AresBatchPartitionReader(partitionReader);
    }
}
