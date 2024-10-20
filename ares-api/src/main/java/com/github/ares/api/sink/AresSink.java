package com.github.ares.api.sink;

import com.github.ares.api.common.PluginIdentifierInterface;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.serialization.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public interface AresSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT>
        extends Serializable,
        PluginIdentifierInterface {

    /**
     * Set the row type info of sink row data. This method will be automatically called by
     * translation.
     *
     * @param aresRowType The row type info of sink.
     */
    @Deprecated
    default void setTypeInfo(AresRowType aresRowType) {
        throw new UnsupportedOperationException("setTypeInfo method is not supported");
    }

    /**
     * Get the data type of the records consumed by this sink.
     *
     * @return Ares data type.
     */
    @Deprecated
    default AresDataType<IN> getConsumedType() {
        throw new UnsupportedOperationException("getConsumedType method is not supported");
    }

    /**
     * This method will be called to creat {@link SinkWriter}
     *
     * @param context The sink context
     * @return Return sink writer instance
     * @throws IOException throws IOException when createWriter failed.
     */
    SinkWriter<IN, CommitInfoT, StateT> createWriter(SinkWriter.Context context) throws IOException;

    default SinkWriter<IN, CommitInfoT, StateT> restoreWriter(
            SinkWriter.Context context, List<StateT> states) throws IOException {
        return createWriter(context);
    }

    /**
     * Get {@link StateT} serializer. So that {@link StateT} can be transferred across processes
     *
     * @return Serializer of {@link StateT}
     */
    default Optional<Serializer<StateT>> getWriterStateSerializer() {
        return Optional.empty();
    }

    /**
     * This method will be called to create {@link SinkCommitter}
     *
     * @return Return sink committer instance
     * @throws IOException throws IOException when createCommitter failed.
     */
    default Optional<SinkCommitter<CommitInfoT>> createCommitter() throws IOException {
        return Optional.empty();
    }

    default void prepare(Config pluginConfig) {
        throw new UnsupportedOperationException("prepare method is not supported");
    }

    /**
     * Get {@link CommitInfoT} serializer. So that {@link CommitInfoT} can be transferred across
     * processes
     *
     * @return Serializer of {@link CommitInfoT}
     */
    default Optional<Serializer<CommitInfoT>> getCommitInfoSerializer() {
        return Optional.empty();
    }

    /**
     * This method will be called to create {@link SinkAggregatedCommitter}
     *
     * @return Return sink aggregated committer instance
     * @throws IOException throws IOException when createAggregatedCommitter failed.
     */
    default Optional<SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT>>
    createAggregatedCommitter() throws IOException {
        return Optional.empty();
    }

    /**
     * Get {@link AggregatedCommitInfoT} serializer. So that {@link AggregatedCommitInfoT} can be
     * transferred across processes
     *
     * @return Serializer of {@link AggregatedCommitInfoT}
     */
    default Optional<Serializer<AggregatedCommitInfoT>> getAggregatedCommitInfoSerializer() {
        return Optional.empty();
    }

    default void truncateTable(String tableName) {
        throw new UnsupportedOperationException("Truncate table is not supported by this sink");
    }
}
