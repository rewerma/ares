package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalStartTransaction;

import java.io.Serializable;

public class StartTransactionExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    public void execute(LogicalStartTransaction startTransaction) {
        traceLogger.info("SQL: START TRANSACTION");
        executorManager.getTransactionManager().start();
    }
}
