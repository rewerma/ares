package com.github.ares.engine.core;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalMergeIntoSQL;
import com.github.ares.parser.plan.LogicalOperation;
import java.util.Map;
import java.util.Optional;

public abstract class MergeIntoSqlExecutor extends AbstractBaseExecutor
        implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.MERGE_INTO_SQL;
    }

    @Override
    public Scope scope() {
        return Scope.DIRECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalMergeIntoSQL) operation, plParams);
        return lastData;
    }

    public void execute(LogicalMergeIntoSQL mergeIntoSQL, PlParams plParams) {
        SinkExecutorSupport.execute(
                executorManager,
                mergeIntoSQL.getSinkTable(),
                mergeIntoSQL.getOriginSQL(),
                (options, factory) -> execute(options, factory, mergeIntoSQL, plParams));
    }

    public abstract void execute(
            Map<String, Object> sinkConfig,
            Optional<? extends Factory> sinkFactory,
            LogicalMergeIntoSQL mergeIntoSQL,
            PlParams plParams);
}
