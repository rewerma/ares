package com.github.ares.engine.spark.core;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;

import com.github.ares.engine.core.CreatePaimonTableExecutor;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.PlParams;
import com.github.ares.parser.plan.LogicalCreatePaimonTable;

public class SparkCreatePaimonTableExecutor extends CreatePaimonTableExecutor {
    private static final long serialVersionUID = -1L;
    private SparkExecutorManager sparkExecutorManager;

    public void init(ExecutorManager executorManager) {
        this.sparkExecutorManager = (SparkExecutorManager) executorManager;
        super.init(executorManager);
    }

    @Override
    public void execute(LogicalCreatePaimonTable createPaimonTable, PlParams plParams) {
        traceLogger.info("SQL: {}", createPaimonTable.getOriginSQL());
        String sql = replaceParams(createPaimonTable.getSql(), plParams);
        PaimonSparkSql.create(
                sparkExecutorManager.getSparkSessionManager().getSparkSession(),
                createPaimonTable.getProperties(),
                sql);
    }
}
