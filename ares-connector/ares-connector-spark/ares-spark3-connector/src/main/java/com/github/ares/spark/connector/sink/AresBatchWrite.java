package com.github.ares.spark.connector.sink;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.sink.SinkAggregatedCommitter;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.spark.connector.sink.write.AresSparkDataWriterFactory;
import com.github.ares.spark.connector.sink.write.AresSparkWriterCommitMessage;
import com.github.ares.spark.connector.statistic.JobStatisticInformation;
import com.github.ares.spark.connector.statistic.WriterStatistic;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AresBatchWrite<StateT, CommitInfoT, AggregatedCommitInfoT>
        implements BatchWrite, StreamingWrite {

    private final AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink;

//    private final SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> aggregatedCommitter;

    private final CatalogTable catalogTable;

    private final long startTimeMillis;

    public AresBatchWrite(
            AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            CatalogTable catalogTable)
            throws IOException {
        this.sink = sink;
        this.catalogTable = catalogTable;
        this.startTimeMillis = System.currentTimeMillis();
//        this.aggregatedCommitter = sink.createAggregatedCommitter().orElse(null);
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new AresSparkDataWriterFactory<>(sink, catalogTable);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        WriterStatistic totalStatistic = aggregateStatistic(messages);
        List<CommitInfoT> commitInfos = collectCommitInfos(messages);
        SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> aggregatedCommitter =
                SinkAggregatedCommitterLoader.load(sink, getClass().getClassLoader());
        SinkAggregatedCommitterLoader.ensureCommitterPresentIfNeeded(
                aggregatedCommitter, commitInfos.size());
        if (aggregatedCommitter != null) {
            try {
                aggregatedCommitter.commit(JsonUtils.toJsonString(commitInfos));
            } catch (IOException e) {
                throw new RuntimeException("SinkAggregatedCommitter commit failed in driver", e);
            }
        }
        JobStatisticInformation.log(
                startTimeMillis,
                totalStatistic.getReadCount(),
                totalStatistic.getWriteCount(),
                totalStatistic.getFailedCount());
    }

    private WriterStatistic aggregateStatistic(WriterCommitMessage[] messages) {
        WriterStatistic totalStatistic = new WriterStatistic();
        if (messages == null) {
            return totalStatistic;
        }
        for (WriterCommitMessage message : messages) {
            if (message instanceof AresSparkWriterCommitMessage) {
                totalStatistic.merge(((AresSparkWriterCommitMessage<?>) message).getStatistic());
            }
        }
        return totalStatistic;
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        List<CommitInfoT> commitInfos = collectCommitInfos(messages);
        SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> aggregatedCommitter =
                SinkAggregatedCommitterLoader.load(sink, getClass().getClassLoader());
        SinkAggregatedCommitterLoader.ensureCommitterPresentIfNeeded(
                aggregatedCommitter, commitInfos.size());
        if (aggregatedCommitter != null) {
            try {
                aggregatedCommitter.abort(JsonUtils.toJsonString(commitInfos));
            } catch (Exception e) {
                throw new RuntimeException("SinkAggregatedCommitter abort failed in driver", e);
            }
        }
    }

    private List<CommitInfoT> collectCommitInfos(WriterCommitMessage[] messages) {
        if (messages == null) {
            return Arrays.asList();
        }
        return Arrays.stream(messages)
                .map(m -> ((AresSparkWriterCommitMessage<CommitInfoT>) m).getMessage())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
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
}
