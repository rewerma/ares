package com.github.ares.connector.source;

import com.github.ares.api.source.SourceEvent;
import com.github.ares.api.source.SourceSplit;
import com.github.ares.api.source.SourceSplitEnumerator;

import java.util.List;
import java.util.Set;

public class CoordinatedEnumeratorContext<SplitT extends SourceSplit>
        implements SourceSplitEnumerator.Context<SplitT> {

    protected final CoordinatedSource<?, SplitT, ?> coordinatedSource;

    public CoordinatedEnumeratorContext(CoordinatedSource<?, SplitT, ?> coordinatedSource) {
        this.coordinatedSource = coordinatedSource;
    }

    @Override
    public int currentParallelism() {
        return coordinatedSource.currentReaderCount();
    }

    @Override
    public Set<Integer> registeredReaders() {
        return coordinatedSource.registeredReaders();
    }

    @Override
    public void assignSplit(int subtaskId, List<SplitT> splits) {
        coordinatedSource.addSplits(subtaskId, splits);
    }

    @Override
    public void signalNoMoreSplits(int subtaskId) {
        coordinatedSource.handleNoMoreSplits(subtaskId);
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        coordinatedSource.handleEnumeratorEvent(subtaskId, event);
    }
}
