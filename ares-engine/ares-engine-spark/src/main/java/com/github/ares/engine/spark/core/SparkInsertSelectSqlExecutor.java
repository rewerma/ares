package com.github.ares.engine.spark.core;

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
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.InsertSelectSqlExecutor;
import com.github.ares.engine.core.PlParams;
import com.github.ares.engine.core.AresSinkFactory;
import com.github.ares.engine.spark.utils.TypeConverterUtils;
import com.github.ares.parser.plan.LogicalInsertSelectSQL;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;


public class SparkInsertSelectSqlExecutor extends InsertSelectSqlExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    private SparkExecutorManager sparkExecutorManager;

    @Inject
    private AresSinkFactory aresSinkFactory;

    public void init(ExecutorManager executorManager) {
        this.sparkExecutorManager = (SparkExecutorManager) executorManager;
        super.init(executorManager);
    }

    @Override
    public void commonFunction() {
    }

    @Override
    public void execute(Map<String, Object> sinkConfig, Optional<? extends Factory> sinkFactory, LogicalInsertSelectSQL isSql, PlParams plParams) {
        if (isSql.getOriginSQL() != null) {
            traceLogger.info("SQL: {}; Params: {}", isSql.getOriginSQL(), plParams);
        }
        SparkSession sparkSession = sparkExecutorManager.getSparkSessionManager().getSparkSession();
        sinkConfig.put(CommonOptions.SINK_TYPE.key(), SinkType.INSERT.name());
        sinkConfig.put(CommonOptions.INSERT_COLUMNS.key(), isSql.getTargetColumns());

        commonFunction();
        String selectSql = replaceParams(isSql.getSelectSQL(), plParams);
        Dataset<Row> resultDf = sparkSession.sql(selectSql);
        if (isSql.getRepartitionNums() != null) {
            resultDf = sparkExecutorManager.getSparkCommonExecutor().repartition(resultDf, isSql.getRepartitionNums(),
                    isSql.getRepartitionColumns());
        }
        if (isSql.getWithShow() != null) {
            traceLogger.info("SQL show result: {}", isSql.getSelectSQL());
            resultDf.show(isSql.getWithShow());
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
        CatalogTable catalogTable = CatalogTable.of(
                tableIdentifier,
                tableSchema,
                new HashMap<>(),
                new ArrayList<>(),
                "");

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        TableSinkFactoryContext context =
                new TableSinkFactoryContext(
                        catalogTable,
                        ReadonlyConfig.fromMap(sinkConfig),
                        classLoader);

        AresSink<?, ?, ?, ?> aresSink = aresSinkFactory.createSink(sinkConfig, sinkFactory, catalogTable, context);
        sparkExecutorManager.getSparkSinkExecutor().sink(resultDf, aresSink, catalogTable);
    }
}
