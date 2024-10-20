package com.github.ares.spark.connector.source.partition.batch;

import org.apache.spark.sql.connector.read.InputPartition;

public class AresBatchInputPartition implements InputPartition {
    private final int partitionId;

    public AresBatchInputPartition(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }
}
