package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalCreateSinkTable;

import java.io.Serializable;

public class CreateSinkTableExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    public void execute(LogicalCreateSinkTable sinkTable) {
        traceLogger.info("Create sink table: {}, connector type: {}",
                sinkTable.getTableName(), sinkTable.getConnector());
    }
}
