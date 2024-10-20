package com.github.ares.connector.file.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.Boundedness;
import com.github.ares.api.source.SourceReader;
import com.github.ares.api.source.SourceSplitEnumerator;
import com.github.ares.api.source.SupportColumnProjection;
import com.github.ares.api.source.SupportParallelism;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.connector.file.config.BaseFileSourceConfig;
import com.github.ares.connector.file.config.BaseMultipleTableFileSourceConfig;
import com.github.ares.connector.file.source.reader.MultipleTableFileSourceReader;
import com.github.ares.connector.file.source.split.FileSourceSplit;
import com.github.ares.connector.file.source.split.MultipleTableFileSourceSplitEnumerator;
import com.github.ares.connector.file.source.state.FileSourceState;

import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseMultipleTableFileSource
        implements AresSource<AresRow, FileSourceSplit, FileSourceState>,
        SupportParallelism,
        SupportColumnProjection {

    private final BaseMultipleTableFileSourceConfig baseMultipleTableFileSourceConfig;

    public BaseMultipleTableFileSource(
            BaseMultipleTableFileSourceConfig baseMultipleTableFileSourceConfig) {
        this.baseMultipleTableFileSourceConfig = baseMultipleTableFileSourceConfig;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public abstract String getPluginName();

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return baseMultipleTableFileSourceConfig.getFileSourceConfigs().stream()
                .map(BaseFileSourceConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<AresRow, FileSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new MultipleTableFileSourceReader(readerContext, baseMultipleTableFileSourceConfig);
    }

    @Override
    public SourceSplitEnumerator<FileSourceSplit, FileSourceState> createEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> enumeratorContext) {
        return new MultipleTableFileSourceSplitEnumerator(
                enumeratorContext, baseMultipleTableFileSourceConfig);
    }

    @Override
    public SourceSplitEnumerator<FileSourceSplit, FileSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> enumeratorContext,
            FileSourceState checkpointState) {
        return new MultipleTableFileSourceSplitEnumerator(
                enumeratorContext, baseMultipleTableFileSourceConfig, checkpointState);
    }
}
