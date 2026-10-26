package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalOperation;
import java.io.Serializable;
import java.util.List;

public class BodyExecutionExecutor extends AbstractBaseExecutor
        implements IBodyExecutor, Serializable {
    private static final long serialVersionUID = -1L;

    public Object execute(List<LogicalOperation> operations, PlParams plParams) {
        Object lastData = null;
        for (LogicalOperation operation : operations) {
            if (operation == null) {
                continue;
            }
            OperationHandler handler =
                    executorManager
                            .getOperationDispatcher()
                            .bodyHandler(operation.getOperationType());
            Object res;
            if (handler != null) {
                res = handler.handle(operation, plParams, lastData, this::execute);
                if (LoopControl.isSignal(res)) {
                    return res;
                }
            } else {
                res =
                        executorManager
                                .getOperationDispatcher()
                                .dispatchDirect(operation, plParams, lastData);
            }
            lastData = res;
        }
        return lastData;
    }
}
