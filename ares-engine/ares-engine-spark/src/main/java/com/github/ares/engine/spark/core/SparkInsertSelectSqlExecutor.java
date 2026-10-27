package com.github.ares.engine.spark.core;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.api.common.SinkType;
import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.PhysicalColumn;
import com.github.ares.api.table.catalog.TableIdentifier;
import com.github.ares.api.table.catalog.TableSchema;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactoryContext;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connector.jdbc.sink.JdbcSink;
import com.github.ares.engine.core.AresSinkFactory;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.InsertSelectSqlExecutor;
import com.github.ares.engine.core.PlParams;
import com.github.ares.engine.spark.utils.TypeConverterUtils;
import com.github.ares.parser.hive.HiveTables;
import com.github.ares.parser.paimon.PaimonTables;
import com.github.ares.parser.plan.LogicalInsertSelectSQL;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

public class SparkInsertSelectSqlExecutor extends InsertSelectSqlExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    private SparkExecutorManager sparkExecutorManager;

    @Inject private AresSinkFactory aresSinkFactory;

    public void init(ExecutorManager executorManager) {
        this.sparkExecutorManager = (SparkExecutorManager) executorManager;
        super.init(executorManager);
    }

    @Override
    public void execute(
            Map<String, Object> sinkConfig,
            Optional<? extends Factory> sinkFactory,
            LogicalInsertSelectSQL isSql,
            PlParams plParams) {
        if (isSql.getOriginSQL() != null) {
            traceLogger.info("SQL: {}; Params: {}", isSql.getOriginSQL(), plParams);
        }
        SparkSession sparkSession = sparkExecutorManager.getSparkSessionManager().getSparkSession();
        sinkConfig.put(CommonOptions.SINK_TYPE.key(), SinkType.INSERT.name());
        sinkConfig.put(CommonOptions.INSERT_COLUMNS.key(), isSql.getTargetColumns());

        String selectSql = replaceParams(isSql.getSelectSQL(), plParams);
        Dataset<Row> resultDf = sparkSession.sql(selectSql);
        if (isSql.getRepartitionNums() != null) {
            resultDf =
                    sparkExecutorManager
                            .getSparkCommonExecutor()
                            .repartition(
                                    resultDf,
                                    isSql.getRepartitionNums(),
                                    isSql.getRepartitionColumns());
        }
        String connector = String.valueOf(sinkConfig.get(CommonOptions.CONNECTOR.key()));
        if (isSql.isOverwrite()
                && (HiveTables.isHiveConnector(connector)
                        || PaimonTables.isPaimonConnector(connector))) {
            throw new AresException(overwriteUnsupportedMessage(connector));
        }
        if (isSql.getWithShow() != null) {
            traceLogger.info("SQL show result: {}", isSql.getSelectSQL());
            resultDf.show(isSql.getWithShow());
        }
        if (HiveTables.isHiveConnector(connector)) {
            HiveSparkSql.insert(
                    sparkSession,
                    HiveTables.tableName(sinkConfig),
                    resultDf,
                    isSql.getTargetColumns());
            return;
        }
        if (PaimonTables.isPaimonConnector(connector)) {
            PaimonSparkSql.insert(sparkSession, sinkConfig, resultDf, isSql.getTargetColumns());
            return;
        }

        StructType structType = resultDf.schema();
        List<Column> columns = new ArrayList<>(structType.fields().length);
        for (StructField structField : structType.fields()) {
            String columnName = structField.name();
            AresDataType<?> aresDataType = TypeConverterUtils.convert(structField.dataType());
            Column column = PhysicalColumn.of(columnName, aresDataType, 0, true, null, null);
            columns.add(column);
        }
        TableSchema.Builder builder = TableSchema.builder();
        TableSchema tableSchema = builder.columns(columns).build();

        TableIdentifier tableIdentifier = TableIdentifier.of("default", "default", "default");
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableIdentifier, tableSchema, new HashMap<>(), new ArrayList<>(), "");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        TableSinkFactoryContext context =
                new TableSinkFactoryContext(
                        catalogTable, ReadonlyConfig.fromMap(sinkConfig), classLoader);
        List<Column> columns2 = new ArrayList<>();
        AresSink<?, ?, ?, ?> aresSink;
        if (!isSql.getTargetColumns().isEmpty()) {
            List<String> targetColumns = isSql.getTargetColumns();

            StructField[] structFields = structType.fields();

            if (targetColumns.size() != structFields.length) {
                throw new AresException("Target columns number not match select columns number");
            }

            int i = 0;
            for (StructField structField : structFields) {
                AresDataType<?> aresDataType = TypeConverterUtils.convert(structField.dataType());
                Column column =
                        PhysicalColumn.of(targetColumns.get(i), aresDataType, 0, true, null, null);
                columns2.add(column);
                i++;
            }

            TableSchema.Builder builder2 = TableSchema.builder();
            TableSchema tableSchema2 = builder2.columns(columns2).build();

            CatalogTable catalogTable2 =
                    CatalogTable.of(
                            tableIdentifier, tableSchema2, new HashMap<>(), new ArrayList<>(), "");
            sinkConfig = new LinkedHashMap<>(sinkConfig);
            sinkConfig.put(CommonOptions.HAS_TARGET_COLUMNS.key(), Boolean.TRUE);
            aresSink = aresSinkFactory.createSink(sinkConfig, sinkFactory, catalogTable2, context);
        } else {
            aresSink = aresSinkFactory.createSink(sinkConfig, sinkFactory, catalogTable, context);
        }

        CatalogTable writeCatalogTable =
                !isSql.getTargetColumns().isEmpty()
                        ? CatalogTable.of(
                                tableIdentifier,
                                TableSchema.builder().columns(columns2).build(),
                                new HashMap<>(),
                                new ArrayList<>(),
                                "")
                        : catalogTable;
        if (isSql.isOverwrite() && isAnalyticsConnector(connector)) {
            overwriteAnalytics(sinkConfig, sinkFactory, resultDf, aresSink, catalogTable, isSql);
            return;
        }
        if (isSql.isOverwrite()) {
            if (!(aresSink instanceof JdbcSink)) {
                throw new AresException(overwriteUnsupportedMessage(aresSink.getPluginName()));
            }
            JdbcOverwriteWriter.execute(
                    sparkExecutorManager,
                    (JdbcSink) aresSink,
                    resultDf,
                    writeCatalogTable,
                    isSql.getSinkTable().getTableName());
            if (!sparkExecutorManager.getTransactionManager().isActive()) {
                reloadWrittenTable(isSql.getSinkTable().getTableName());
            }
            return;
        }
        if (sparkExecutorManager.tryTransactionalSink(
                aresSink, resultDf, writeCatalogTable, isSql.getSinkTable().getTableName())) {
            return;
        }
        sparkExecutorManager.getSparkSinkExecutor().sink(resultDf, aresSink, catalogTable);

        reloadWrittenTable(isSql.getSinkTable().getTableName());
    }

    /**
     * Doris and StarRocks replace the whole table by truncating, then using the normal insert path.
     * The query is cached first so a failed select does not clear the table.
     */
    private void overwriteAnalytics(
            Map<String, Object> sinkConfig,
            Optional<? extends Factory> sinkFactory,
            Dataset<Row> resultDf,
            AresSink<?, ?, ?, ?> insertSink,
            CatalogTable catalogTable,
            LogicalInsertSelectSQL isSql) {
        sparkExecutorManager.getTransactionManager().ensureNotActive("INSERT OVERWRITE");
        String tableName = isSql.getSinkTable().getTableName();
        Dataset<Row> cached = resultDf.persist(StorageLevel.MEMORY_AND_DISK());
        try {
            cached.count();
            truncateAnalytics(sinkConfig, sinkFactory, tableName);
            sparkExecutorManager.getSparkSinkExecutor().sink(cached, insertSink, catalogTable);
            reloadWrittenTable(tableName);
        } finally {
            cached.unpersist();
        }
    }

    private void truncateAnalytics(
            Map<String, Object> sinkConfig,
            Optional<? extends Factory> sinkFactory,
            String tableName) {
        Map<String, Object> truncateConfig = new LinkedHashMap<>(sinkConfig);
        truncateConfig.put(CommonOptions.SINK_TYPE.key(), SinkType.TRUNCATE.name());
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                Collections.singletonList(
                                        new PhysicalColumn("none", null, null, null)))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("default", "default", "default"),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "");
        TableSinkFactoryContext context =
                new TableSinkFactoryContext(
                        catalogTable, ReadonlyConfig.fromMap(truncateConfig), classLoader);
        AresSink<?, ?, ?, ?> truncateSink =
                aresSinkFactory.createSink(truncateConfig, sinkFactory, catalogTable, context);
        traceLogger.info("INSERT OVERWRITE truncates {}", tableName);
        truncateSink.truncateTable(tableName);
    }

    private void reloadWrittenTable(String tableName) {
        if (executorManager.getSourceTables().containsKey(tableName.toLowerCase(Locale.ROOT))) {
            executorManager.getReloadFunctionExecutor().reloadSourceTable(tableName);
        }
    }

    private static boolean isAnalyticsConnector(String connector) {
        return "doris".equalsIgnoreCase(connector) || "starrocks".equalsIgnoreCase(connector);
    }

    private static String overwriteUnsupportedMessage(String target) {
        return "INSERT OVERWRITE is only supported for JDBC sinks, got: " + target;
    }
}
