package com.github.ares.engine.core;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalCommit;
import com.github.ares.parser.plan.LogicalOperation;

public class CommitExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.COMMIT;
    }

    @Override
    public Scope scope() {
        return Scope.BODY;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalCommit) operation);
        return lastData;
    }

    public void execute(LogicalCommit commit) {
        traceLogger.info("SQL: COMMIT");
        executorManager.getTransactionManager().commit();
    }
}
