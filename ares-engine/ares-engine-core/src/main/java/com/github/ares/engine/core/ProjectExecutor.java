package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalAnonymousBody;
import com.github.ares.parser.plan.LogicalCreateFunction;
import com.github.ares.parser.plan.LogicalCreateProcedure;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalOperation;

import java.io.Serializable;
import java.util.List;

public class ProjectExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    private Object lastData = null;

    public Object execute(List<LogicalOperation> operations) {
        for (LogicalOperation operation : operations) {
            if (operation == null) {
                continue;
            }
            switch (operation.getOperationType()) {
                case CREATE_SOURCE_TABLE:
                    executorManager.getCreateSourceTableExecutor().execute((LogicalCreateSourceTable) operation);
                    break;
                case CREATE_SINK_TABLE:
                    executorManager.getCreateSinkTableExecutor().execute((LogicalCreateSinkTable) operation);
                    break;
                case ANONYMOUS_BODY: {
                    Object lastResult = executorManager.getAnonymousBodyExecutor().execute((LogicalAnonymousBody) operation);
                    if (lastResult != null) {
                        lastData = lastResult;
                    }
                    break;
                }
                case CREATE_PROCEDURE:
                    executorManager.getCreateProcedureExecutor().execute((LogicalCreateProcedure) operation);
                    break;
                case CREATE_FUNCTION:
                    executorManager.createFunctionExecutor.execute((LogicalCreateFunction) operation);
                    break;
                default: {
                    Object lastResult = executorManager.directExecutionExecutor.execute(operation, new PlParams(), lastData);
                    if (lastResult != null) {
                        lastData = lastResult;
                    }
                    break;
                }
            }
        }
        return lastData;
    }
}
