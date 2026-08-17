package com.github.ares.engine.core;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalStartTransaction;

public class StartTransactionExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.START_TRANSACTION;
    }

    @Override
    public Scope scope() {
        return Scope.BODY;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalStartTransaction) operation);
        return lastData;
    }

    public void execute(LogicalStartTransaction startTransaction) {
        traceLogger.info("SQL: START TRANSACTION");
        executorManager.getTransactionManager().start();
    }
}
