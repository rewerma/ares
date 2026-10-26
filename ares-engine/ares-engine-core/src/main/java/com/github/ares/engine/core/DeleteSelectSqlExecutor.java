package com.github.ares.engine.core;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalDeleteSelectSQL;
import com.github.ares.parser.plan.LogicalOperation;
import java.util.Map;
import java.util.Optional;

public abstract class DeleteSelectSqlExecutor extends AbstractBaseExecutor
        implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.DELETE_SELECT_SQL;
    }

    @Override
    public Scope scope() {
        return Scope.DIRECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalDeleteSelectSQL) operation, plParams);
        return lastData;
    }

    public void execute(LogicalDeleteSelectSQL deleteSelectSQL, PlParams plParams) {
        SinkExecutorSupport.execute(
                executorManager,
                deleteSelectSQL.getSinkTable(),
                deleteSelectSQL.getOriginSQL(),
                (options, factory) -> execute(options, factory, deleteSelectSQL, plParams));
    }

    public abstract void execute(
            Map<String, Object> sinkConfig,
            Optional<? extends Factory> sinkFactory,
            LogicalDeleteSelectSQL dsSql,
            PlParams plParams);
}
