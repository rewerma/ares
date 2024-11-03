package com.github.ares.parser.visitor;

import com.github.ares.common.engine.PlType;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalCallFunction;
import com.github.ares.parser.plan.LogicalCreateProcedure;
import com.github.ares.parser.plan.LogicalExpression;
import com.github.ares.parser.plan.LogicalOperation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PlCallStatementVisitor {
    private PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public LogicalOperation visitCallStatement(PlSqlParser.Call_statementContext callStatementContext,
                                               Map<String, PlType> allParams,
                                               List<LogicalOperation> baseBody,
                                               List<String> structs) {
        String functionName = callStatementContext.routine_name().getText();
        return visitCallStatement(functionName, callStatementContext.function_argument(), allParams, baseBody, structs);
    }

    public LogicalOperation visitCallStatement(String functionName, PlSqlParser.Function_argumentContext functionArgumentContext,
                                               Map<String, PlType> allParams, List<LogicalOperation> baseOperations, List<String> structs) {
        List<Integer> outArgsIdx = null;
        for (LogicalOperation baseOperation : baseOperations) {
            if (baseOperation instanceof LogicalCreateProcedure &&
                    ((LogicalCreateProcedure) baseOperation).getProcedureName().equals(functionName)) {
                outArgsIdx = ((LogicalCreateProcedure) baseOperation).getOutArgsIndex();
                break;
            }
        }

        List<Integer> outArgsIdxTmp = outArgsIdx;
        Map<String, PlType> outParams = null;
        if (functionArgumentContext != null) {
            outParams = new LinkedHashMap<>();
            List<PlSqlParser.ArgumentContext> argumentContexts = functionArgumentContext.argument();
            int i = 0;
            for (PlSqlParser.ArgumentContext argumentContext : argumentContexts) {
                i++;
                if (outArgsIdxTmp != null && outArgsIdxTmp.contains(i - 1)
                        && allParams.containsKey(argumentContext.getText())) {
                    outParams.put(argumentContext.getText(), allParams.get(argumentContext.getText()));
                }
            }
        }

        List<LogicalExpression> args = new ArrayList<>();
        if (functionArgumentContext != null) {
            List<PlSqlParser.ArgumentContext> argumentContexts = functionArgumentContext.argument();
            int i = 0;
            for (PlSqlParser.ArgumentContext argumentContext : argumentContexts) {
                i++;
                if (outArgsIdxTmp != null && outArgsIdxTmp.contains(i - 1)) {
                    continue;
                }
                LogicalExpression expr = visitorManager.getExpressionVisitor().visitExpressionContext(argumentContext.expression(), allParams, structs);
                args.add(expr);
            }
        }

        LogicalCallFunction callFunction = new LogicalCallFunction();
        callFunction.setFuncName(functionName);
        callFunction.setArgs(args);

        if (outParams != null) {
            List<Argument> arguments = new ArrayList<>();
            outParams.forEach((name, type) -> {
                Argument argument = new Argument(name, type);
                arguments.add(argument);
            });
            callFunction.setOutArgs(arguments);
        }
        return callFunction;
    }
}
