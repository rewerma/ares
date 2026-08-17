package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalOperation;
import java.io.Serializable;
import java.util.List;

public class DirectExecutionExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    public Object execute(List<LogicalOperation> operations) {
        PlParams plParams = new PlParams();
        Object lastData = null;
        for (LogicalOperation operation : operations) {
            if (operation == null) {
                continue;
            }
            lastData = execute(operation, plParams, lastData);
        }
        return lastData;
    }

    public Object execute(LogicalOperation operation, PlParams plParams, Object lastData) {
        return executorManager
                .getOperationDispatcher()
                .dispatchDirect(operation, plParams, lastData);
    }
}
