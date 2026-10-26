package com.github.ares.engine.spark.core;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;

import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.PlParams;
import com.github.ares.engine.core.SparkNativeSqlExecutor;
import com.github.ares.parser.plan.LogicalSparkNativeSql;

public class SparkExecuteNativeSqlExecutor extends SparkNativeSqlExecutor {
    private static final long serialVersionUID = -1L;
    private SparkExecutorManager sparkExecutorManager;

    public void init(ExecutorManager executorManager) {
        this.sparkExecutorManager = (SparkExecutorManager) executorManager;
        super.init(executorManager);
    }

    @Override
    public void execute(LogicalSparkNativeSql sparkNativeSql, PlParams plParams) {
        traceLogger.info("SQL: {}", sparkNativeSql.getOriginSQL());
        String sql = replaceParams(sparkNativeSql.getSql(), plParams);
        sparkExecutorManager.getSparkSessionManager().getSparkSession().sql(sql);
    }
}
