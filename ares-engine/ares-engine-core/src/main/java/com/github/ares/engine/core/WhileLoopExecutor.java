package com.github.ares.engine.core;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalWhileLoop;

public class WhileLoopExecutor extends AbstractBaseExecutor implements OperationHandler {

    @Override
    public OperationType handledType() {
        return OperationType.WHILE_LOOP;
    }

    @Override
    public Scope scope() {
        return Scope.BODY;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        return execute((LogicalWhileLoop) operation, plParams, body);
    }

    public Object execute(LogicalWhileLoop whileLoop, PlParams plParams, BodyCallback body) {
        traceLogger.info("While loop: {} BEGIN", whileLoop.getCondition().getExpr());
        String expr = whileLoop.getCondition().getExpr();
        Object lastData = null;
        while (executorManager.getExpressionExecutor().execute4Bool(expr, plParams)) {
            Object res = body.invoke(whileLoop.getWhileBody(), plParams);
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
        traceLogger.info("While loop: {} END", whileLoop.getCondition().getExpr());
        return lastData;
    }
}
