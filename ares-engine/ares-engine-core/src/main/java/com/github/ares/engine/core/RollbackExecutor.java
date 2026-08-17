package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalRollback;

import java.io.Serializable;

public class RollbackExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    public void execute(LogicalRollback rollback) {
        traceLogger.info("SQL: ROLLBACK");
        executorManager.getTransactionManager().rollback();
    }
}
