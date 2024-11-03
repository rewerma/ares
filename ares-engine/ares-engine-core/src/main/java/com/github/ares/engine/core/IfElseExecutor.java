package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalIfElse;
import com.github.ares.parser.plan.LogicalOperation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;
import static com.github.ares.parser.enums.OperationType.CONTINUE_LOOP;
import static com.github.ares.parser.enums.OperationType.EXIT_LOOP;
import static com.github.ares.parser.enums.OperationType.IF_ELSE;
import static com.github.ares.parser.enums.OperationType.RETURN_VALUE;

public class IfElseExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;


    public static final Integer BREAK_LOOP_FLAG = -1;
    public static final int RETURN_FUNCTION_FLAG = -2;
    public static final int CONTINUE_LOOP_FLAG = -3;

    public int ifElse(LogicalIfElse ifElse, PlParams plParams, int i,
                      List<LogicalOperation> operations, BodyCallback body) {
        List<LogicalIfElse> ifElseOperations = new ArrayList<>();
        LogicalOperation baseOperationTmp = null;
        int j = i;
        do {
            if (baseOperationTmp != null) {
                ifElse = (LogicalIfElse) baseOperationTmp;
            }
            ifElseOperations.add(ifElse);
            j++;
            if (j < operations.size()) {
                baseOperationTmp = operations.get(j);
            } else {
                break;
            }
        } while (baseOperationTmp.getOperationType() == IF_ELSE &&
                (((LogicalIfElse) baseOperationTmp).getIsElseIf() != null ||
                        ((LogicalIfElse) baseOperationTmp).getIsElse() != null));

        Object exitFlag = ifElse(ifElseOperations, plParams, body);
        if (EXIT_LOOP == exitFlag) {
            return BREAK_LOOP_FLAG; // with loop exit
        }
        if (CONTINUE_LOOP == exitFlag) {
            return CONTINUE_LOOP_FLAG; // with loop continue
        }
        if (RETURN_VALUE == exitFlag) {
            return RETURN_FUNCTION_FLAG; // with return function
        }
        return i + ifElseOperations.size() - 1;
    }

    private Object ifElse(List<LogicalIfElse> ifElseOperations, PlParams plParams, BodyCallback body) {
        Iterator<LogicalIfElse> it = ifElseOperations.iterator();
        LogicalIfElse ifElse = it.next();
        String expr = replaceParams(ifElse.getCondition().getExpr(), plParams);
        if (executorManager.getExpressionExecutor().execute4Bool(expr)) {
            traceLogger.info("IF body: {} BEGIN", ifElse.getCondition().getExpr());
            Object result = body.invoke(ifElse.getIfBody(), plParams);
            traceLogger.info("IF body: {} END", ifElse.getCondition().getExpr());
            return result;
        } else {
            return elseIfOperation(it, ifElse, plParams, body);
        }
    }

    private Object elseIfOperation(Iterator<LogicalIfElse> itIfElse, LogicalIfElse parentIfElse, PlParams plParams, BodyCallback body) {
        if (!itIfElse.hasNext()) {
            return null;
        }
        LogicalIfElse ifElse = itIfElse.next();
        if (ifElse.getIsElseIf() != null) {
            String expr = replaceParams(ifElse.getCondition().getExpr(), plParams);
            if (executorManager.getExpressionExecutor().execute4Bool(expr)) {
                traceLogger.info("IF body: {} BEGIN", ifElse.getCondition().getExpr());
                Object result = body.invoke(ifElse.getIfBody(), plParams);
                traceLogger.info("IF body: {} END", ifElse.getCondition().getExpr());
                return result;
            } else {
                return elseIfOperation(itIfElse, ifElse, plParams, body);
            }
        } else {
            traceLogger.info("IF-ELSE body:  NOT ( {} ) BEGIN", parentIfElse.getCondition().getExpr());
            Object result = body.invoke(ifElse.getIfBody(), plParams);
            traceLogger.info("IF-ELSE body: NOT ( {} ) END", parentIfElse.getCondition().getExpr());
            return result;
        }
    }
}
