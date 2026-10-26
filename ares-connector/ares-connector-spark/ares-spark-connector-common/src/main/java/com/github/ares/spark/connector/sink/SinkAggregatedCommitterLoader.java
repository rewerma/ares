package com.github.ares.spark.connector.sink;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.sink.SinkAggregatedCommitter;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.utils.PluginClassLoader;
import java.io.IOException;

public final class SinkAggregatedCommitterLoader {

    private SinkAggregatedCommitterLoader() {}

    public static <StateT, CommitInfoT, AggregatedCommitInfoT>
            SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> load(
                    AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
                    ClassLoader parentClassLoader) {
        try {
            AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> pluginSink =
                    PluginClassLoader.reloadInPluginClassLoader(sink, parentClassLoader);
            return pluginSink.createAggregatedCommitter().orElse(null);
        } catch (IOException e) {
            throw new AresException(
                    "Failed to create SinkAggregatedCommitter for plugin " + sink.getPluginName(),
                    e);
        }
    }

    public static void ensureCommitterPresentIfNeeded(
            SinkAggregatedCommitter<?, ?> committer, int commitInfoCount) {
        if (committer == null && commitInfoCount > 0) {
            throw new AresException(
                    "SinkAggregatedCommitter is required to finalize "
                            + commitInfoCount
                            + " writer commit message(s), but it could not be created");
        }
    }
}
