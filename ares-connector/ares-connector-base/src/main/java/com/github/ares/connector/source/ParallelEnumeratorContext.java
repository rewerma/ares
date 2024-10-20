package com.github.ares.connector.source;

import com.github.ares.api.source.SourceEvent;
import com.github.ares.api.source.SourceSplit;
import com.github.ares.api.source.SourceSplitEnumerator;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ParallelEnumeratorContext<SplitT extends SourceSplit>
        implements SourceSplitEnumerator.Context<SplitT> {

    protected final ParallelSource<?, SplitT, ?> parallelSource;
    protected final Integer parallelism;
    protected final Integer subtaskId;
    protected volatile boolean running = false;

    public ParallelEnumeratorContext(
            ParallelSource<?, SplitT, ?> parallelSource, int parallelism, int subtaskId) {
        this.parallelSource = parallelSource;
        this.parallelism = parallelism;
        this.subtaskId = subtaskId;
    }

    @Override
    public int currentParallelism() {
        return parallelism;
    }

    @Override
    public Set<Integer> registeredReaders() {
        return running ? Collections.singleton(subtaskId) : Collections.emptySet();
    }

    public void register() {
        running = true;
    }

    @Override
    public void assignSplit(int subtaskId, List<SplitT> splits) {
        if (this.subtaskId == subtaskId) {
            parallelSource.addSplits(splits);
        }
    }

    @Override
    public void signalNoMoreSplits(int subtaskId) {
        if (this.subtaskId == subtaskId) {
            parallelSource.handleNoMoreSplits();
        }
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        throw new UnsupportedOperationException(
                "Flink ParallelSource don't support sending SourceEvent. "
                        + "Please implement the `SupportCoordinate` marker interface on the Ares source.");
    }
}
