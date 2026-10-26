package com.github.ares.engine.core;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalRollback;

public class RollbackExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.ROLLBACK;
    }

    @Override
    public Scope scope() {
        return Scope.BODY;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalRollback) operation);
        return lastData;
    }

    public void execute(LogicalRollback rollback) {
        traceLogger.info("SQL: ROLLBACK");
        executorManager.getTransactionManager().rollback();
    }
}
