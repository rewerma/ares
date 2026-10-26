package com.github.ares.connector.starrocks.febe;

import com.github.ares.api.common.JobContext;
import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.Boundedness;
import com.github.ares.api.source.SourceReader;
import com.github.ares.api.source.SourceSplitEnumerator;
import com.github.ares.api.source.SupportColumnProjection;
import com.github.ares.api.source.SupportParallelism;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.configuration.ReadonlyConfig;
import java.util.Collections;
import java.util.List;

public class StarRocksFeBeSource
        implements AresSource<AresRow, StarRocksSplit, StarRocksSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    private static final long serialVersionUID = 1L;

    private final StarRocksFeBeConfig config;
    private final CatalogTable catalogTable;

    public StarRocksFeBeSource(ReadonlyConfig options) {
        StarRocksFeBeConfig.checkMode(options);
        this.config = StarRocksFeBeConfig.from(options);
        this.catalogTable = StarRocksSchemaResolver.resolve(config);
        this.config.setColumns(StarRocksSchemaResolver.columnNames(catalogTable));
    }

    @Override
    public String getPluginName() {
        return "StarRocks";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<AresRow, StarRocksSplit> createReader(SourceReader.Context readerContext) {
        AresRowType rowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        return new StarRocksFeBeSourceReader(readerContext, config, rowType);
    }

    @Override
    public SourceSplitEnumerator<StarRocksSplit, StarRocksSourceState> createEnumerator(
            SourceSplitEnumerator.Context<StarRocksSplit> enumeratorContext) {
        return new StarRocksFeBeSplitEnumerator(enumeratorContext, config, Collections.emptySet());
    }

    @Override
    public SourceSplitEnumerator<StarRocksSplit, StarRocksSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<StarRocksSplit> enumeratorContext,
            StarRocksSourceState checkpointState) {
        return new StarRocksFeBeSplitEnumerator(
                enumeratorContext, config, checkpointState.getAssignedSplitIds());
    }

    @Override
    public void setJobContext(JobContext jobContext) {}
}
