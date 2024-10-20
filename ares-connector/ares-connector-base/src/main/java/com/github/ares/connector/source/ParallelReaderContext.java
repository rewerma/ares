package com.github.ares.connector.source;

import com.github.ares.api.source.Boundedness;
import com.github.ares.api.source.SourceEvent;
import com.github.ares.api.source.SourceReader;

public class ParallelReaderContext implements SourceReader.Context {

    protected final ParallelSource<?, ?, ?> parallelSource;
    protected final Boundedness boundedness;
    protected final Integer subtaskId;

    public ParallelReaderContext(
            ParallelSource<?, ?, ?> parallelSource, Boundedness boundedness, Integer subtaskId) {
        this.parallelSource = parallelSource;
        this.boundedness = boundedness;
        this.subtaskId = subtaskId;
    }

    @Override
    public int getIndexOfSubtask() {
        return subtaskId;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public void signalNoMoreElement() {
        parallelSource.handleNoMoreElement();
    }

    @Override
    public void sendSplitRequest() {
        parallelSource.handleSplitRequest(subtaskId);
    }

    @Override
    public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
        throw new UnsupportedOperationException(
                "Flink ParallelSource don't support sending SourceEvent. "
                        + "Please implement the `SupportCoordinate` marker interface on the Ares source.");
    }
}
