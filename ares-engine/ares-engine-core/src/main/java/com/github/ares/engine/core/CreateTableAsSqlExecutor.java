package com.github.ares.engine.core;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalCreateTableAsSQL;
import com.github.ares.parser.plan.LogicalOperation;

public abstract class CreateTableAsSqlExecutor extends AbstractBaseExecutor
        implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.CREATE_TABLE_AS_SQL;
    }

    @Override
    public Scope scope() {
        return Scope.DIRECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalCreateTableAsSQL) operation, plParams);
        return lastData;
    }

    public abstract void execute(LogicalCreateTableAsSQL createTableAsSql, PlParams plParams);
}
