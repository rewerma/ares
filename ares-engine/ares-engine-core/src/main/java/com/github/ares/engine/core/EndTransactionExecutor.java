package com.github.ares.engine.core;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalEndTransaction;
import com.github.ares.parser.plan.LogicalOperation;

public class EndTransactionExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.END_TRANSACTION;
    }

    @Override
    public Scope scope() {
        return Scope.BODY;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalEndTransaction) operation);
        return lastData;
    }

    public void execute(LogicalEndTransaction endTransaction) {
        traceLogger.info("SQL: END TRANSACTION");
        executorManager.getTransactionManager().end();
    }
}
