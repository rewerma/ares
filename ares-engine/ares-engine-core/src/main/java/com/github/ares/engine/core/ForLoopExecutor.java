package com.github.ares.engine.core;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalForLoop;
import com.github.ares.parser.plan.LogicalOperation;

public class ForLoopExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.FOR_LOOP;
    }

    @Override
    public Scope scope() {
        return Scope.BODY;
    }

    @Override
    public Object handle(
            LogicalOperation operation,
            PlParams plParams,
            Object lastData,
            BodyCallback bodyCallback) {
        return execute((LogicalForLoop) operation, plParams, bodyCallback);
    }

    public Object execute(LogicalForLoop forLoop, PlParams plParams, BodyCallback bodyCallback) {
        traceLogger.info("For loop: {} BEGIN", forLoop.conditionString());
        Number lowerVal =
                (Number)
                        executorManager
                                .getExpressionExecutor()
                                .execute(forLoop.getLowerExpr().getExpr(), plParams);
        Number upperVal =
                (Number)
                        executorManager
                                .getExpressionExecutor()
                                .execute(forLoop.getUpperExpr().getExpr(), plParams);
        Object lastData = null;
        try {
            for (int i = lowerVal.intValue(); i <= upperVal.intValue(); i++) {
                plParams.put(forLoop.getIndexName(), i, PlType.of(InternalFieldType.INT));
                Object res = bodyCallback.invoke(forLoop.getForBody(), plParams);
                if (LoopControl.isReturn(res)) {
                    return res;
                }
                if (LoopControl.isExit(res)) {
                    break;
                }
                if (!LoopControl.isContinue(res)) {
                    lastData = res;
                }
            }
        } finally {
            plParams.remove(forLoop.getIndexName());
        }
        traceLogger.info("For loop: {} END", forLoop.conditionString());
        return lastData;
    }
}
