package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalCommit;

import java.io.Serializable;

public class CommitExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    public void execute(LogicalCommit commit) {
        traceLogger.info("SQL: COMMIT");
        executorManager.getTransactionManager().commit();
    }
}
