package com.github.ares.engine.core;

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
            lastData =
                    executorManager.getOperationDispatcher().dispatchProject(operation, lastData);
        }
        return lastData;
    }
}
