package com.github.ares.spark.connector.sink.write;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

public class AresWriteBuilder<StateT, CommitInfoT, AggregatedCommitInfoT>
        implements WriteBuilder {

    private final AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink;
    private final CatalogTable catalogTable;

    public AresWriteBuilder(
            AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            CatalogTable catalogTable) {
        this.sink = sink;
        this.catalogTable = catalogTable;
    }

    @Override
    public Write build() {
        return new AresWrite<>(sink, catalogTable);
    }
}
