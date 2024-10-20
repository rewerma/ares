package com.github.ares.engine.spark.core;

import com.github.ares.engine.core.CreateFunctionExecutor;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.sql.function.DynamicFunction;

public class SparkCreateFunctionExecutor extends CreateFunctionExecutor {
    private static final long serialVersionUID = 1L;

    private SparkExecutorManager sparkExecutorManager;

    public void init(ExecutorManager executorManager) {
        this.sparkExecutorManager = (SparkExecutorManager) executorManager;
        super.init(executorManager);
    }

    @Override
    public void registerFunction(DynamicFunction dynamicFunction) {
        sparkExecutorManager.getUdfManager().registerUdf(dynamicFunction);
    }
}
