package com.github.ares.spark.connector.sink;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.sink.SinkAggregatedCommitter;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.spark.connector.sink.write.AresSparkDataWriterFactory;
import com.github.ares.spark.connector.sink.write.AresSparkWriterCommitMessage;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AresBatchWrite<StateT, CommitInfoT, AggregatedCommitInfoT>
        implements BatchWrite, StreamingWrite {

    private final AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink;

    private final SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> aggregatedCommitter;

    private final CatalogTable catalogTable;

    public AresBatchWrite(
            AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            CatalogTable catalogTable)
            throws IOException {
        this.sink = sink;
        this.catalogTable = catalogTable;
        this.aggregatedCommitter = sink.createAggregatedCommitter().orElse(null);
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new AresSparkDataWriterFactory<>(sink, catalogTable);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        if (aggregatedCommitter != null) {
            try {
                aggregatedCommitter.commit(combineCommitMessage(messages));
            } catch (IOException e) {
                throw new RuntimeException("SinkAggregatedCommitter commit failed in driver", e);
            }
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        if (aggregatedCommitter != null) {
            try {
                aggregatedCommitter.abort(combineCommitMessage(messages));
            } catch (Exception e) {
                throw new RuntimeException("SinkAggregatedCommitter abort failed in driver", e);
            }
        }
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        return (StreamingDataWriterFactory) createBatchWriterFactory(info);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        commit(messages);
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        abort(messages);
    }

    private List<AggregatedCommitInfoT> combineCommitMessage(WriterCommitMessage[] messages) {
        if (aggregatedCommitter == null || messages.length == 0) {
            return Collections.emptyList();
        }
        List<CommitInfoT> commitInfos =
                Arrays.stream(messages)
                        .map(m -> ((AresSparkWriterCommitMessage<CommitInfoT>) m).getMessage())
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        return Collections.singletonList(aggregatedCommitter.combine(commitInfos));
    }
}
