package com.github.ares.connector.sink;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.sink.SinkAggregatedCommitter;
import com.github.ares.api.sink.SinkCommitter;
import com.github.ares.api.sink.SinkWriter;
import com.github.ares.common.serialization.Serializer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public abstract class AbstractSimpleSink<T, StateT>
        implements AresSink<T, StateT, Void, Void> {

    @Override
    public abstract AbstractSinkWriter<T, StateT> createWriter(SinkWriter.Context context)
            throws IOException;

    @Override
    public SinkWriter<T, Void, StateT> restoreWriter(
            SinkWriter.Context context, List<StateT> states) throws IOException {
        return createWriter(context);
    }

    @Override
    public final Optional<SinkCommitter<Void>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public final Optional<Serializer<Void>> getCommitInfoSerializer() {
        return Optional.empty();
    }

    @Override
    public final Optional<SinkAggregatedCommitter<Void, Void>> createAggregatedCommitter()
            throws IOException {
        return Optional.empty();
    }

    @Override
    public final Optional<Serializer<Void>> getAggregatedCommitInfoSerializer() {
        return Optional.empty();
    }
}
