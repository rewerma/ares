package com.github.ares.engine.core;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.parser.plan.LogicalForLoop;

import java.io.Serializable;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;
import static com.github.ares.parser.enums.OperationType.EXIT_LOOP;

public class ForLoopExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    public Object execute(LogicalForLoop forLoop, PlParams plParams, BodyCallback bodyCallback) {
        traceLogger.info("For loop: {} BEGIN", forLoop.conditionString());
        String lowerExpr = replaceParams(forLoop.getLowerExpr().getExpr(), plParams);
        Number lowerVal = (Number) executorManager.getExpressionExecutor().execute(lowerExpr);
        String upperExpr = replaceParams(forLoop.getUpperExpr().getExpr(), plParams);
        Number upperVal = (Number) executorManager.getExpressionExecutor().execute(upperExpr);
        Object res = null;
        for (int i = lowerVal.intValue(); i <= upperVal.intValue(); i++) {
            plParams.put(forLoop.getIndexName(), i, PlType.of(InternalFieldType.INT));
            res = bodyCallback.invoke(forLoop.getForBody(), plParams);
            if (EXIT_LOOP == res) {
                break;
            }
        }
        plParams.remove(forLoop.getIndexName());
        traceLogger.info("For loop: {} END", forLoop.conditionString());
        return res;
    }
}
