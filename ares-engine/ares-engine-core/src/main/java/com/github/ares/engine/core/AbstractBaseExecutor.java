package com.github.ares.engine.core;

import java.io.Serializable;

public class AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    protected TraceLogger traceLogger;

    protected ExecutorManager executorManager;

    public void init(ExecutorManager executorManager) {
        this.executorManager = executorManager;
        this.traceLogger = executorManager.getTraceLogger();
    }
}
