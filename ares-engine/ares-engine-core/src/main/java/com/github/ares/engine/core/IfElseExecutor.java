package com.github.ares.engine.core;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalExpression;
import com.github.ares.parser.plan.LogicalIfElse;
import com.github.ares.parser.plan.LogicalOperation;
import java.util.List;

public class IfElseExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.IF_ELSE;
    }

    @Override
    public Scope scope() {
        return Scope.BODY;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        Object result = execute((LogicalIfElse) operation, plParams, body);
        if (LoopControl.isSignal(result)) {
            return result;
        }
        return lastData;
    }

    public Object execute(LogicalIfElse ifElse, PlParams plParams, BodyCallback body) {
        if (matches(ifElse.getCondition(), plParams)) {
            return runIfBody(ifElse.getCondition().getExpr(), ifElse.getIfBody(), plParams, body);
        }
        String lastFailed = ifElse.getCondition().getExpr();
        if (ifElse.getElseIfs() != null) {
            for (LogicalIfElse elseIf : ifElse.getElseIfs()) {
                if (matches(elseIf.getCondition(), plParams)) {
                    return runIfBody(
                            elseIf.getCondition().getExpr(), elseIf.getIfBody(), plParams, body);
                }
                lastFailed = elseIf.getCondition().getExpr();
            }
        }
        if (ifElse.getElseBody() != null) {
            traceLogger.info("IF-ELSE body:  NOT ( {} ) BEGIN", lastFailed);
            Object result = body.invoke(ifElse.getElseBody(), plParams);
            traceLogger.info("IF-ELSE body: NOT ( {} ) END", lastFailed);
            return result;
        }
        return null;
    }

    private boolean matches(LogicalExpression condition, PlParams plParams) {
        return executorManager.getExpressionExecutor().execute4Bool(condition.getExpr(), plParams);
    }

    private Object runIfBody(
            String expr, List<LogicalOperation> branchBody, PlParams plParams, BodyCallback body) {
        traceLogger.info("IF body: {} BEGIN", expr);
        Object result = body.invoke(branchBody, plParams);
        traceLogger.info("IF body: {} END", expr);
        return result;
    }
}
