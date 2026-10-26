package com.github.ares.connector.doris.febe;

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

public class DorisFeBeSource
        implements AresSource<AresRow, DorisSplit, DorisSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    private static final long serialVersionUID = 1L;

    private final DorisFeBeConfig config;
    private final CatalogTable catalogTable;

    public DorisFeBeSource(ReadonlyConfig options) {
        DorisFeBeConfig.checkMode(options);
        this.config = DorisFeBeConfig.from(options);
        this.catalogTable = DorisSchemaResolver.resolve(config);
        this.config.setColumns(DorisSchemaResolver.columnNames(catalogTable));
    }

    @Override
    public String getPluginName() {
        return "Doris";
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
    public SourceReader<AresRow, DorisSplit> createReader(SourceReader.Context readerContext) {
        AresRowType rowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        return new DorisFeBeSourceReader(readerContext, config, rowType);
    }

    @Override
    public SourceSplitEnumerator<DorisSplit, DorisSourceState> createEnumerator(
            SourceSplitEnumerator.Context<DorisSplit> enumeratorContext) {
        return new DorisFeBeSplitEnumerator(enumeratorContext, config, Collections.emptySet());
    }

    @Override
    public SourceSplitEnumerator<DorisSplit, DorisSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<DorisSplit> enumeratorContext,
            DorisSourceState checkpointState) {
        return new DorisFeBeSplitEnumerator(
                enumeratorContext, config, checkpointState.getAssignedSplitIds());
    }

    @Override
    public void setJobContext(JobContext jobContext) {}
}
