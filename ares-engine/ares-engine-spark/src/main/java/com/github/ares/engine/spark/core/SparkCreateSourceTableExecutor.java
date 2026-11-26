package com.github.ares.engine.spark.core;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SourceTableInfo;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.utils.Constants;
import com.github.ares.common.utils.SerializationUtils;
import com.github.ares.engine.core.CreateSourceTableExecutor;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.spark.utils.TypeConverterUtils;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import java.util.Locale;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class SparkCreateSourceTableExecutor extends CreateSourceTableExecutor {
    private static final long serialVersionUID = -1L;

    private SparkExecutorManager sparkExecutorManager;

    @Override
    public void init(ExecutorManager executorManager) {
        this.sparkExecutorManager = (SparkExecutorManager) executorManager;
        super.init(executorManager);
    }

    @Override
    public void loadSource(String tableName, SourceTableInfo sourceTableInfo) {
        AresSource<?, ?, ?> source = sourceTableInfo.getSource();
        try {
            LogicalCreateSourceTable sourceTable = createSourceTables.get(tableName);
            if (sourceTable != null) {
                source.prepare(
                        ReadonlyConfig.fromMap(sourceTable.getSourceTableConfig()).toConfig());
            }
        } catch (UnsupportedOperationException e) {
            // ignore
        }
        StructType schema = (StructType) TypeConverterUtils.convert(source.getProducedType());
        int parallelism = resolveParallelism(tableName);
        traceLogger.info("Load source table: {}, parallelism: {}", tableName, parallelism);
        Dataset<Row> dataset =
                sparkExecutorManager
                        .getSparkSessionManager()
                        .getSparkSession()
                        .read()
                        .format(AresSource.class.getSimpleName())
                        .option(
                                Constants.SOURCE_SERIALIZATION,
                                SerializationUtils.objectToString(source))
                        .option(CommonOptions.PARALLELISM.key(), String.valueOf(parallelism))
                        .schema(schema)
                        .load();
        dataset.createOrReplaceTempView(tableName);
    }

    private int resolveParallelism(String tableName) {
        LogicalCreateSourceTable sourceTable = createSourceTables.get(tableName);
        if (sourceTable == null && tableName != null) {
            sourceTable = createSourceTables.get(tableName.toLowerCase(Locale.ROOT));
        }
        return SourceReadParallelism.resolve(
                sourceTable, executorManager.getPlProperties().getProperties());
    }
}
