package com.github.ares.engine.spark.core;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;

import com.github.ares.engine.core.CreateHiveTableExecutor;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.PlParams;
import com.github.ares.parser.plan.LogicalCreateHiveTable;

public class SparkCreateHiveTableExecutor extends CreateHiveTableExecutor {
    private static final long serialVersionUID = -1L;
    private SparkExecutorManager sparkExecutorManager;

    public void init(ExecutorManager executorManager) {
        this.sparkExecutorManager = (SparkExecutorManager) executorManager;
        super.init(executorManager);
    }

    @Override
    public void execute(LogicalCreateHiveTable createHiveTable, PlParams plParams) {
        traceLogger.info("SQL: {}", createHiveTable.getOriginSQL());
        String sql = replaceParams(createHiveTable.getSql(), plParams);
        HiveSparkSql.create(sparkExecutorManager.getSparkSessionManager().getSparkSession(), sql);
    }
}
