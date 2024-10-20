package com.github.ares.api.table.connector;

import com.github.ares.api.sink.AresSink;

public interface TableSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> {

    AresSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> createSink();
}
