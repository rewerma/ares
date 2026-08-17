package com.github.ares.engine.core;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalUpdateSelectSQL;
import java.util.Map;
import java.util.Optional;

public abstract class UpdateSelectSqlExecutor extends AbstractBaseExecutor
        implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.UPDATE_SELECT_SQL;
    }

    @Override
    public Scope scope() {
        return Scope.DIRECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalUpdateSelectSQL) operation, plParams);
        return lastData;
    }

    public void execute(LogicalUpdateSelectSQL updateSelectSQL, PlParams plParams) {
        SinkExecutorSupport.execute(
                executorManager,
                updateSelectSQL.getSinkTable(),
                updateSelectSQL.getOriginSQL(),
                (options, factory) -> execute(options, factory, updateSelectSQL, plParams));
    }

    public abstract void execute(
            Map<String, Object> sinkConfig,
            Optional<? extends Factory> sinkFactory,
            LogicalUpdateSelectSQL usSql,
            PlParams plParams);
}
