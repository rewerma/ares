package com.github.ares.engine.core;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalInsertSelectSQL;
import com.github.ares.parser.plan.LogicalOperation;
import java.util.Map;
import java.util.Optional;

public abstract class InsertSelectSqlExecutor extends AbstractBaseExecutor
        implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.INSERT_SELECT_SQL;
    }

    @Override
    public Scope scope() {
        return Scope.DIRECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalInsertSelectSQL) operation, plParams);
        return lastData;
    }

    public void execute(LogicalInsertSelectSQL insertSelectSQL, PlParams plParams) {
        SinkExecutorSupport.execute(
                executorManager,
                insertSelectSQL.getSinkTable(),
                insertSelectSQL.getOriginSQL(),
                (options, factory) -> execute(options, factory, insertSelectSQL, plParams));
    }

    public abstract void execute(
            Map<String, Object> sinkConfig,
            Optional<? extends Factory> sinkFactory,
            LogicalInsertSelectSQL isSql,
            PlParams plParams);
}
