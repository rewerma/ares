package com.github.ares.connector.file.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.Boundedness;
import com.github.ares.api.source.SourceReader;
import com.github.ares.api.source.SourceSplitEnumerator;
import com.github.ares.api.source.SupportColumnProjection;
import com.github.ares.api.source.SupportParallelism;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.source.reader.ReadStrategy;
import com.github.ares.connector.file.source.split.FileSourceSplit;
import com.github.ares.connector.file.source.split.FileSourceSplitEnumerator;
import com.github.ares.connector.file.source.state.FileSourceState;

import java.util.List;

public abstract class BaseFileSource
        implements AresSource<AresRow, FileSourceSplit, FileSourceState>,
        SupportParallelism,
        SupportColumnProjection {
    protected AresRowType rowType;
    protected ReadStrategy readStrategy;
    protected HadoopConf hadoopConf;
    protected List<String> filePaths;
    protected Config pluginConfig;

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public AresDataType<AresRow> getProducedType() {
        return rowType;
    }

    @Override
    public SourceReader<AresRow, FileSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new BaseFileSourceReader(readStrategy, readerContext);
    }

    @Override
    public SourceSplitEnumerator<FileSourceSplit, FileSourceState> createEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> enumeratorContext) throws Exception {
        refreshFilePaths();
        return new FileSourceSplitEnumerator(enumeratorContext, filePaths);
    }

    @Override
    public SourceSplitEnumerator<FileSourceSplit, FileSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> enumeratorContext,
            FileSourceState checkpointState)
            throws Exception {
        refreshFilePaths();
        return new FileSourceSplitEnumerator(enumeratorContext, filePaths, checkpointState);
    }

    public void refreshFilePaths() {
    }
}
