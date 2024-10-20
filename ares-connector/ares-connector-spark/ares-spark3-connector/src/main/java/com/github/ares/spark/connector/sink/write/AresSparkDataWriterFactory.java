package com.github.ares.spark.connector.sink.write;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.sink.DefaultSinkWriterContext;
import com.github.ares.api.sink.SinkCommitter;
import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.exceptions.AresException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;

import java.io.IOException;

public class AresSparkDataWriterFactory<CommitInfoT, StateT>
        implements DataWriterFactory, StreamingDataWriterFactory {

    private final AresSink<AresRow, StateT, CommitInfoT, ?> sink;
    private final CatalogTable catalogTable;

    public AresSparkDataWriterFactory(
            AresSink<AresRow, StateT, CommitInfoT, ?> sink, CatalogTable catalogTable) {
        this.sink = sink;
        this.catalogTable = catalogTable;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        SinkWriter.Context context = new DefaultSinkWriterContext((int) taskId);
        SinkWriter<AresRow, CommitInfoT, StateT> writer;
        SinkCommitter<CommitInfoT> committer;
        try {
            writer = sink.createWriter(context);
        } catch (IOException e) {
            throw new AresException("Failed to create SinkWriter.", e);
        }
        try {
            committer = sink.createCommitter().orElse(null);
        } catch (IOException e) {
            throw new AresException("Failed to create SinkCommitter.", e);
        }
        return new AresSparkDataWriter<>(
                writer, committer, catalogTable.getAresRowType(), 0);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        return createWriter(partitionId, taskId);
    }
}
