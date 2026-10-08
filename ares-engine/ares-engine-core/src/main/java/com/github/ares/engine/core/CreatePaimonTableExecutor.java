package com.github.ares.engine.core;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalCreatePaimonTable;
import com.github.ares.parser.plan.LogicalOperation;

public abstract class CreatePaimonTableExecutor extends AbstractBaseExecutor
        implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.CREATE_PAIMON_TABLE;
    }

    @Override
    public Scope scope() {
        return Scope.DIRECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalCreatePaimonTable) operation, plParams);
        return lastData;
    }

    public abstract void execute(LogicalCreatePaimonTable createPaimonTable, PlParams plParams);
}
