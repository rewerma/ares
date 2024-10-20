package com.github.ares.spark.connector.source.partition.batch;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.IOException;

public class AresBatchPartitionReader implements PartitionReader<InternalRow> {

    private final ParallelBatchPartitionReader partitionReader;

    public AresBatchPartitionReader(ParallelBatchPartitionReader partitionReader) {
        this.partitionReader = partitionReader;
    }

    @Override
    public boolean next() throws IOException {
        return partitionReader.next();
    }

    @Override
    public InternalRow get() {
        return partitionReader.get();
    }

    @Override
    public void close() throws IOException {
        partitionReader.close();
    }
}
