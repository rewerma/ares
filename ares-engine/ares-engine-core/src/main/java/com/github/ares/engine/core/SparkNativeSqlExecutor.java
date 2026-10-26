package com.github.ares.engine.core;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalSparkNativeSql;

public abstract class SparkNativeSqlExecutor extends AbstractBaseExecutor
        implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.SPARK_NATIVE_SQL;
    }

    @Override
    public Scope scope() {
        return Scope.DIRECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalSparkNativeSql) operation, plParams);
        return lastData;
    }

    public abstract void execute(LogicalSparkNativeSql sparkNativeSql, PlParams plParams);
}
