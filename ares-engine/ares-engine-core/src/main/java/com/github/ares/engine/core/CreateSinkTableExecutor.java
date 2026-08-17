package com.github.ares.engine.core;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalOperation;

public class CreateSinkTableExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.CREATE_SINK_TABLE;
    }

    @Override
    public Scope scope() {
        return Scope.PROJECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalCreateSinkTable) operation);
        return lastData;
    }

    public void execute(LogicalCreateSinkTable sinkTable) {
        traceLogger.info(
                "Create sink table: {}, connector type: {}",
                sinkTable.getTableName(),
                sinkTable.getConnector());
    }
}
