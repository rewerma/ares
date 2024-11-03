package com.github.ares.engine.spark.core;

import com.github.ares.com.google.inject.Singleton;
import com.github.ares.engine.core.CreateTableAsSqlExecutor;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.PlParams;
import com.github.ares.parser.plan.LogicalCreateTableAsSQL;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;


public class SparkCreateTableAsSqlExecutor extends CreateTableAsSqlExecutor {
    private static final long serialVersionUID = -1L;
    private SparkExecutorManager sparkExecutorManager;

    public void init(ExecutorManager executorManager) {
        this.sparkExecutorManager = (SparkExecutorManager) executorManager;
        super.init(executorManager);
    }

    @Override
    public void execute(LogicalCreateTableAsSQL createTableAsSql, PlParams plParams) {
        traceLogger.info("SQL: {}", createTableAsSql.getOriginSQL());
        String sql = createTableAsSql.getSelectSQL();
        sql = replaceParams(sql, plParams);
        Dataset<Row> resultDf = sparkExecutorManager.getSparkSessionManager().getSparkSession().sql(sql);
        if (createTableAsSql.getRepartitionNums() != null) {
            resultDf = sparkExecutorManager.getSparkCommonExecutor().repartition(resultDf, createTableAsSql.getRepartitionNums(),
                    createTableAsSql.getRepartitionColumns());
        }
        if (createTableAsSql.getWithCache() != null) {
            resultDf = sparkExecutorManager.getSparkCommonExecutor().cache(resultDf, createTableAsSql.getTableName());
        }
        if (createTableAsSql.getWithShow() != null) {
            traceLogger.info("SQL show result: {}", createTableAsSql.getSelectSQL());
            resultDf.show(createTableAsSql.getWithShow());
        }
        resultDf.createOrReplaceTempView(createTableAsSql.getTableName());
    }
}
