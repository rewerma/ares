package com.github.ares.engine.core;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalSelectIntoSQL;

public abstract class SelectIntoSqlExecutor extends AbstractBaseExecutor
        implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.SELECT_INTO_SQL;
    }

    @Override
    public Scope scope() {
        return Scope.BODY;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalSelectIntoSQL) operation, plParams);
        return lastData;
    }

    public abstract void execute(LogicalSelectIntoSQL selectIntoSQL, PlParams plParams);
}
