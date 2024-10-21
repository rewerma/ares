package com.github.ares.connector.sink;

import com.github.ares.api.sink.SinkWriter;

import java.util.Optional;

public abstract class AbstractSinkWriter<T, StateT> implements SinkWriter<T, Void, StateT> {

    @Override
    public Optional<Void> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public final void abortPrepare() {
        // nothing
    }
}
