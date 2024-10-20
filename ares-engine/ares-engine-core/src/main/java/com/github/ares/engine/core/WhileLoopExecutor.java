package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalWhileLoop;

import java.io.Serializable;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;
import static com.github.ares.parser.enums.OperationType.EXIT_LOOP;

public class WhileLoopExecutor extends AbstractBaseExecutor implements Serializable {

    public Object execute(LogicalWhileLoop whileLoop, PlParams plParams, BodyCallback body) {
        traceLogger.info("While loop: {} BEGIN", whileLoop.getCondition().getExpr());
        String expr = replaceParams(whileLoop.getCondition().getExpr(), plParams);
        Object res = null;
        while (executorManager.getExpressionExecutor().execute4Bool(expr)) {
            res = body.invoke(whileLoop.getWhileBody(), plParams);
            if (EXIT_LOOP == res) {
                break;
            }
            expr = replaceParams(whileLoop.getCondition().getExpr(), plParams);
        }
        traceLogger.info("While loop: {} END", whileLoop.getCondition().getExpr());
        return res;
    }
}
