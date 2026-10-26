package com.github.ares.engine.core;

import com.github.ares.common.exceptions.AresException;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalCallFunction;
import com.github.ares.parser.plan.LogicalExpression;
import com.github.ares.parser.plan.LogicalOperation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CallFunctionExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.CALL_FUNCTION;
    }

    @Override
    public Scope scope() {
        return Scope.DIRECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalCallFunction) operation, plParams);
        return lastData;
    }

    public void execute(LogicalCallFunction callFunction, PlParams plParams) {
        Map<String, CreateProcedureFunc> procedures = executorManager.getProcedures();
        if ("reload".equalsIgnoreCase(callFunction.getFuncName())) {
            if (callFunction.getArgs().size() != 1) {
                throw new AresException(
                        String.format(
                                "Invalid number of arguments for function reload. Expected: %d; Found: %d",
                                1, callFunction.getArgs().size()));
            }
            LogicalExpression argExpr = callFunction.getArgs().get(0);
            Object val =
                    executorManager.getExpressionExecutor().execute(argExpr.getExpr(), plParams);
            if (val == null) {
                throw new AresException("Reload function argument cannot be null.");
            }
            if (!(val instanceof String)) {
                throw new AresException(
                        String.format(
                                "Invalid type of argument for function reload. Expected: String; Found: %s",
                                val.getClass().getSimpleName()));
            }
            executorManager.getReloadFunctionExecutor().reloadSourceTable((String) val);
        } else if (procedures.containsKey(callFunction.getFuncName().toLowerCase())) {
            executorManager.getCallProcedureExecutor().execute(callFunction, procedures, plParams);
        } else {
            traceLogger.info(
                    "Call function: {} ( {} )",
                    callFunction.getFuncName(),
                    callFunction.getArgsString());
            List<Object> args = new ArrayList<>();
            for (LogicalExpression argExpr : callFunction.getArgs()) {
                args.add(
                        executorManager
                                .getExpressionExecutor()
                                .execute(argExpr.getExpr(), plParams));
            }
            executorManager
                    .getExpressionExecutor()
                    .invokeFunction(callFunction.getFuncName(), args);
        }
    }
}
