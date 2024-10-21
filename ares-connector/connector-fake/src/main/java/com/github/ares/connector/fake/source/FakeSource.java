package com.github.ares.connector.fake.source;

import com.github.ares.api.common.JobContext;
import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.Boundedness;
import com.github.ares.api.source.SourceReader;
import com.github.ares.api.source.SourceSplitEnumerator;
import com.github.ares.api.source.SupportColumnProjection;
import com.github.ares.api.source.SupportParallelism;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.utils.JobMode;
import com.github.ares.connector.fake.config.FakeConfig;
import com.github.ares.connector.fake.config.MultipleTableFakeSourceConfig;
import com.github.ares.connector.fake.state.FakeSourceState;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FakeSource
        implements AresSource<AresRow, FakeSourceSplit, FakeSourceState>,
        SupportParallelism,
        SupportColumnProjection {

    private JobContext jobContext;
    private final MultipleTableFakeSourceConfig multipleTableFakeSourceConfig;

    public FakeSource(ReadonlyConfig readonlyConfig) {
        this.multipleTableFakeSourceConfig = new MultipleTableFakeSourceConfig(readonlyConfig);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return multipleTableFakeSourceConfig.getFakeConfigs().stream()
                .map(FakeConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceSplitEnumerator<FakeSourceSplit, FakeSourceState> createEnumerator(
            SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext) {
        return new FakeSourceSplitEnumerator(
                enumeratorContext, multipleTableFakeSourceConfig, Collections.emptySet());
    }

    @Override
    public SourceSplitEnumerator<FakeSourceSplit, FakeSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext,
            FakeSourceState checkpointState) {
        return new FakeSourceSplitEnumerator(
                enumeratorContext,
                multipleTableFakeSourceConfig,
                checkpointState.getAssignedSplits());
    }

    @Override
    public SourceReader<AresRow, FakeSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new FakeSourceReader(readerContext, multipleTableFakeSourceConfig);
    }

    @Override
    public String getPluginName() {
        return "fake";
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}
