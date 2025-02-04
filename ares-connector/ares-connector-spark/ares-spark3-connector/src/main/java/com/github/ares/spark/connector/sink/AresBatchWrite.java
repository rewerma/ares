package com.github.ares.spark.connector.sink;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.sink.SinkAggregatedCommitter;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.utils.IsolatedClassLoader;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.common.utils.SerializationUtils;
import com.github.ares.spark.connector.sink.write.AresSparkDataWriterFactory;
import com.github.ares.spark.connector.sink.write.AresSparkWriterCommitMessage;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AresBatchWrite<StateT, CommitInfoT, AggregatedCommitInfoT>
        implements BatchWrite, StreamingWrite {

    private final AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink;

//    private final SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> aggregatedCommitter;

    private final CatalogTable catalogTable;

    public AresBatchWrite(
            AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            CatalogTable catalogTable)
            throws IOException {
        this.sink = sink;
        this.catalogTable = catalogTable;
//        this.aggregatedCommitter = sink.createAggregatedCommitter().orElse(null);
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new AresSparkDataWriterFactory<>(sink, catalogTable);
    }

    private SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> getAggregatedCommitter() {
        try {
            URL jarUrl = sink.getClass().getProtectionDomain().getCodeSource().getLocation();
            if (jarUrl.getFile().endsWith(".jar")) {
                String sinkSerialization = SerializationUtils.objectToString(sink);
                ClassLoader isolatedLoader =
                        new IsolatedClassLoader(new URL[]{jarUrl}, getClass().getClassLoader());
                byte[] sinkBytes = Base64.getDecoder().decode(sinkSerialization);
                AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> aresSink
                        = SerializationUtils.deserialize(sinkBytes, isolatedLoader);

                return aresSink.createAggregatedCommitter().orElse(null);
            }
            return sink.createAggregatedCommitter().orElse(null);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> aggregatedCommitter = getAggregatedCommitter();
        if (aggregatedCommitter != null) {
            try {
                List<CommitInfoT> commitInfos =
                        Arrays.stream(messages)
                                .map(m -> ((AresSparkWriterCommitMessage<CommitInfoT>) m).getMessage())
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());
                String jsonStr = JsonUtils.toJsonString(commitInfos);
                aggregatedCommitter.commit(/*combineCommitMessage(messages)*/jsonStr);
            } catch (IOException e) {
                throw new RuntimeException("SinkAggregatedCommitter commit failed in driver", e);
            }
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> aggregatedCommitter = getAggregatedCommitter();
        if (aggregatedCommitter != null) {
            try {
                List<CommitInfoT> commitInfos =
                        Arrays.stream(messages)
                                .map(m -> ((AresSparkWriterCommitMessage<CommitInfoT>) m).getMessage())
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());
                String jsonStr = JsonUtils.toJsonString(commitInfos);
                aggregatedCommitter.abort(jsonStr);
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
}
