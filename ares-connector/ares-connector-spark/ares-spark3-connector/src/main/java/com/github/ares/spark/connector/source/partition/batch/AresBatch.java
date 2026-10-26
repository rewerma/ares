package com.github.ares.spark.connector.source.partition.batch;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SupportCoordinate;
import com.github.ares.api.table.type.AresRow;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.util.LongAccumulator;

/** A physical plan of Ares source */
public class AresBatch implements Batch {
    private final LongAccumulator readAccumulator;

    private final AresSource<AresRow, ?, ?> source;

    private final int parallelism;
    private final Map<String, String> envOptions;

    public AresBatch(
            AresSource<AresRow, ?, ?> source, int parallelism, Map<String, String> envOptions) {
        this.source = source;
        this.parallelism = parallelism;
        this.envOptions = envOptions;
        SparkSession spark = SparkSession.active();
        this.readAccumulator = spark.sparkContext().longAccumulator("totalReadRows");
    }

    @Override
    public InputPartition[] planInputPartitions() {
        InputPartition[] partitions;
        if (source instanceof SupportCoordinate) {
            partitions = new AresBatchInputPartition[1];
            partitions[0] = new AresBatchInputPartition(0);
        } else {
            partitions = new AresBatchInputPartition[parallelism];
            for (int partitionId = 0; partitionId < parallelism; partitionId++) {
                partitions[partitionId] = new AresBatchInputPartition(partitionId);
            }
        }
        return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new AresBatchPartitionReaderFactory(
                source, parallelism, envOptions, readAccumulator);
    }
}
