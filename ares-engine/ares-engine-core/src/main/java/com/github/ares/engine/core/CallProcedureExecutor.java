package com.github.ares.engine.core;

import com.github.ares.common.exceptions.AresException;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalCallFunction;
import com.github.ares.parser.plan.LogicalExpression;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;

public class CallProcedureExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    public List<Serializable> evaluate(LogicalCallFunction callFunction, Map<String, CreateProcedureFunc> functions, PlParams plParams) {
        CreateProcedureFunc createProcedureFunc = functions.get(callFunction.getFuncName().toLowerCase());
        List<Serializable> args = new ArrayList<>();

        for (LogicalExpression expr : callFunction.getArgs()) {
            String expression = replaceParams(expr.getExpr(), plParams);
            Serializable value = executorManager.getExpressionExecutor().execute(expression);
            args.add(value);
        }
        return createProcedureFunc.evaluate(args);
    }

    public void execute(LogicalCallFunction callFunction, Map<String, CreateProcedureFunc> procedures, PlParams plParams) {
        // call procedure
        if (procedures.containsKey(callFunction.getFuncName().toLowerCase())) {
            traceLogger.info("Call procedure: {} ( {} )", callFunction.getFuncName(), callFunction.getArgsString());
            List<Serializable> result = evaluate(callFunction, procedures, plParams);
            if (result.size() != callFunction.getOutArgs().size()) {
                throw new AresException("The number of out arguments of the procedure '" + callFunction.getFuncName() + "' does not match");
            }
            int i = 0;
            for (Serializable outVal : result) {
                if (outVal != null) {
                    Argument outArg = callFunction.getOutArgs().get(i);
                    plParams.put(outArg.getName(), outVal, outArg.getPlType());
                }
                i++;
            }
        }
    }
}
