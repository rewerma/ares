package com.github.ares.engine.core;

import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.utils.DateTimeUtils;
import com.github.ares.parser.plan.LogicalCallFunction;
import com.github.ares.parser.plan.LogicalExpression;
import com.github.ares.sql.expression.sql.ExpressionEngine;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.ares.engine.core.ExpressionExecutor.rawToHex;
import static com.github.ares.engine.utils.EngineUtil.handleQuoteIdentifier;
import static com.github.ares.engine.utils.EngineUtil.replaceParams;

public class CallFunctionExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    public void execute(LogicalCallFunction callFunction, PlParams plParams) {
        Map<String, CreateProcedureFunc> procedures = executorManager.getProcedures();
        if ("reload".equalsIgnoreCase(callFunction.getFuncName())) {
            if (callFunction.getArgs().size() != 1) {
                throw new AresException(
                        String.format(
                                "Invalid number of arguments for function reload. Expected: %d; Found: %d"
                                , 1, callFunction.getArgs().size()));
            }
            LogicalExpression argExpr = callFunction.getArgs().get(0);
            String argExprStr = replaceParams(argExpr.getExpr(), plParams);
            Object val = executorManager.getExpressionExecutor().execute(argExprStr);
            if (val == null) {
                throw new AresException("Reload function argument cannot be null.");
            }
            if (!(val instanceof String)) {
                throw new AresException(
                        String.format(
                                "Invalid type of argument for function reload. Expected: String; Found: %s"
                                , val.getClass().getSimpleName()));
            }
            executorManager.getReloadFunctionExecutor().reloadSourceTable((String) val);
        } else if (procedures.containsKey(callFunction.getFuncName().toLowerCase())) {
            executorManager.getCallProcedureExecutor().execute(callFunction, procedures, plParams);
        } else {
            executorManager.getExpressionExecutor().getExpressionEngine()
                    .initDynamicFunctions(executorManager.getUdfManager().getDynamicFunctions());
            if (executorManager.getExpressionExecutor().getExpressionEngine()
                    .getAllFunctions().containsKey(callFunction.getFuncName().toUpperCase())) {
                traceLogger.info("Call function: {} ( {} )", callFunction.getFuncName(), callFunction.getArgsString());

                String expression = callFunction.getFuncName() + "(";
                List<String> values = new ArrayList<>();

                for (int i = 0; i < callFunction.getArgs().size(); i++) {
                    LogicalExpression argExpr = callFunction.getArgs().get(i);
                    String argExprStr = replaceParams(argExpr.getExpr(), plParams);
                    Object val = executorManager.getExpressionExecutor().execute(argExprStr);
                    if (val == null) {
                        values.add(null);
                    } else {
                        if (!(val instanceof Number)) {
                            if (val instanceof String || val instanceof LocalDate) {
                                val = handleQuoteIdentifier(val);
                            } else if (val instanceof LocalDateTime) {
                                LocalDateTime dateTime = (LocalDateTime) val;
                                val = DateTimeUtils.localDateTimeToString(dateTime);
                                val = handleQuoteIdentifier(val);
                            } else if (val instanceof byte[]) {
                                val = handleQuoteIdentifier(rawToHex(val));
                            } else {
                                val = handleQuoteIdentifier(val.toString());
                            }
                        }
                        values.add(val.toString());
                    }
                }
                expression += String.join(", ", values) + ")";
                executorManager.getExpressionExecutor().execute(expression);
            } else {
                throw new AresException(String.format("Procedure or function undefined: %s", callFunction.getFuncName()));
            }
        }
    }
}
