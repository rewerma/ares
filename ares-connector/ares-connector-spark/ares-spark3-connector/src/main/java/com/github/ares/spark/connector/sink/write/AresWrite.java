package com.github.ares.spark.connector.sink.write;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.spark.connector.sink.AresBatchWrite;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

import java.io.IOException;

public class AresWrite<AggregatedCommitInfoT, CommitInfoT, StateT> implements Write {

    private final AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink;
    private final CatalogTable catalogTable;

    public AresWrite(
            AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            CatalogTable catalogTable) {
        this.sink = sink;
        this.catalogTable = catalogTable;
    }

    @Override
    public BatchWrite toBatch() {
        try {
            return new AresBatchWrite<>(sink, catalogTable);
        } catch (IOException e) {
            throw new RuntimeException("Ares Spark sink create batch failed", e);
        }
    }

    @Override
    public StreamingWrite toStreaming() {
        try {
            return new AresBatchWrite<>(sink, catalogTable);
        } catch (IOException e) {
            throw new RuntimeException("Ares Spark sink create batch failed", e);
        }
    }
}
