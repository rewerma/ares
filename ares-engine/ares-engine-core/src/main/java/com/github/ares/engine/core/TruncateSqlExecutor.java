package com.github.ares.engine.core;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.api.common.SinkType;
import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.PhysicalColumn;
import com.github.ares.api.table.catalog.TableIdentifier;
import com.github.ares.api.table.catalog.TableSchema;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactoryContext;
import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalTruncateSQL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TruncateSqlExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Inject private AresSinkFactory aresSinkFactory;

    @Override
    public OperationType handledType() {
        return OperationType.TRUNCATE_SQL;
    }

    @Override
    public Scope scope() {
        return Scope.DIRECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalTruncateSQL) operation);
        return lastData;
    }

    public void execute(LogicalTruncateSQL truncateSQL) {
        traceLogger.info("SQL: {}", truncateSQL.getOriginSQL());
        LogicalCreateSinkTable sinkTable = truncateSQL.getSinkTable();
        Optional<? extends Factory> sinkFactory =
                SinkExecutorSupport.requireSinkFactory(executorManager, sinkTable);
        executorManager.getTransactionManager().ensureNotActive("TRUNCATE");
        Map<String, Object> sinkConfig = sinkTable.getOptions();
        sinkConfig.put(CommonOptions.SINK_TYPE.key(), SinkType.TRUNCATE.name());
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        TableSchema.Builder builder = TableSchema.builder();
        TableSchema tableSchema =
                builder.columns(Arrays.asList(new PhysicalColumn("none", null, null, null)))
                        .build();

        TableIdentifier tableIdentifier = TableIdentifier.of("default", "default", "default");
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableIdentifier, tableSchema, new HashMap<>(), new ArrayList<>(), "");

        TableSinkFactoryContext context =
                new TableSinkFactoryContext(
                        catalogTable, ReadonlyConfig.fromMap(sinkConfig), classLoader);
        AresSink<?, ?, ?, ?> aresSink =
                aresSinkFactory.createSink(sinkConfig, sinkFactory, catalogTable, context);

        aresSink.truncateTable(truncateSQL.getSinkTable().getTableName());

        // reload target table
        if (executorManager
                .getSourceTables()
                .containsKey(truncateSQL.getSinkTable().getTableName().toLowerCase())) {
            executorManager
                    .getReloadFunctionExecutor()
                    .reloadSourceTable(truncateSQL.getSinkTable().getTableName());
        }
    }
}
