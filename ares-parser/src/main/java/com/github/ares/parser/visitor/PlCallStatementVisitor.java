package com.github.ares.parser.visitor;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.model.BaseSqlOption;
import com.github.ares.parser.plan.LogicalCallFunction;
import com.github.ares.parser.plan.LogicalCreateProcedure;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalCreateTableAsSQL;
import com.github.ares.parser.plan.LogicalExpression;
import com.github.ares.parser.plan.LogicalInsertSelectSQL;
import com.github.ares.parser.plan.LogicalMergeIntoSQL;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalUpdateSelectSQL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.ares.parser.enums.OperationType.CREATE_SOURCE_TABLE;
import static com.github.ares.parser.enums.OperationType.CREATE_TABLE_AS_SQL;
import static com.github.ares.parser.enums.OperationType.INSERT_SELECT_SQL;
import static com.github.ares.parser.enums.OperationType.MERGE_INTO_SQL;
import static com.github.ares.parser.enums.OperationType.UPDATE_SELECT_SQL;

public class PlCallStatementVisitor {
    private static final String INNER_FUNC_REPARTITION = "repartition";
    public static final String INNER_FUNC_CACHE = "cache";
    public static final String INNER_FUNC_SHOW = "show";

    private PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public LogicalOperation visitCallStatement(PlSqlParser.Call_statementContext call_statementContext,
                                               Map<String, PlType> allParams, List<LogicalOperation> baseBody,
                                               List<LogicalOperation> currentBody, List<String> structs) {
        String functionName = call_statementContext.routine_name().getText();
//        if (INNER_FUNC_REPARTITION.equalsIgnoreCase(functionName)) {
//            return repartition(call_statementContext.function_argument(), currentBody);
//        } else if (INNER_FUNC_CACHE.equalsIgnoreCase(functionName)) {
//            return cacheFunc(currentBody);
//        } else if (INNER_FUNC_SHOW.equalsIgnoreCase(functionName)) {
//            return showFunc(call_statementContext.function_argument(), currentBody);
//        } else {
        return visitCallStatement(functionName, call_statementContext.function_argument(), allParams, baseBody, structs);
//        }
    }

    public LogicalOperation visitCallStatement(String functionName, PlSqlParser.Function_argumentContext function_argumentContext,
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
        if (function_argumentContext != null) {
            outParams = new LinkedHashMap<>();
            List<PlSqlParser.ArgumentContext> argumentContexts = function_argumentContext.argument();
            int i = 0;
            for (PlSqlParser.ArgumentContext argumentContext : argumentContexts) {
                i++;
                if (outArgsIdxTmp != null && outArgsIdxTmp.contains(i - 1)) {
                    if (allParams.containsKey(argumentContext.getText())) {
                        outParams.put(argumentContext.getText(), allParams.get(argumentContext.getText()));
                    }
                }
            }
        }

        List<LogicalExpression> args = new ArrayList<>();
        if (function_argumentContext != null) {
            List<PlSqlParser.ArgumentContext> argumentContexts = function_argumentContext.argument();
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

//    public LogicalOperation repartition(PlSqlParser.Function_argumentContext function_argumentContext, List<LogicalOperation> baseOperations) {
//        if (function_argumentContext == null || (function_argumentContext.argument().size() != 1 && function_argumentContext.argument().size() != 2)) {
//            throw new ParseException("The params of function 'repartition' must be one or two");
//        }
//        LogicalOperation baseOperation = baseOperations.get(baseOperations.size() - 1);
//        if (baseOperation.getOperationType() == INSERT_SELECT_SQL) {
//            repartition(function_argumentContext, (LogicalInsertSelectSQL) baseOperation);
//        } else if (baseOperation.getOperationType() == UPDATE_SELECT_SQL) {
//            repartition(function_argumentContext, (LogicalUpdateSelectSQL) baseOperation);
//        } else if (baseOperation.getOperationType() == MERGE_INTO_SQL) {
//            repartition(function_argumentContext, (LogicalMergeIntoSQL) baseOperation);
//        } else if (baseOperation.getOperationType() == CREATE_TABLE_AS_SQL) {
//            repartition(function_argumentContext, (LogicalCreateTableAsSQL) baseOperation);
//        } else {
//            throw new UnsupportedOperationException("The REPARTITION function only supports `insert...select...`, `update...select...`, " +
//                    "`merge into...`, `create table as...` SQL statements.");
//        }
//        return null;
//    }

//    private static void repartition(PlSqlParser.Function_argumentContext function_argumentContext, BaseSqlOption baseSqlOption) {
//        int partitionNums = Integer.parseInt(function_argumentContext.argument().get(0).getText());
//        baseSqlOption.setRepartitionNums(partitionNums);
//        if (function_argumentContext.argument().size() == 2) {
//            String columns = function_argumentContext.argument().get(1).getText();
//            if (!columns.startsWith("'") || !columns.endsWith("'")) {
//                throw new ParseException("The params of function 'repartition' columns must be VARCHAR");
//            }
//            columns = columns.substring(0, columns.length() - 1).substring(1);
//            String[] cols = columns.split(",");
//            List<String> colList = Arrays.stream(cols).map(String::trim).collect(Collectors.toList());
//            baseSqlOption.setRepartitionColumns(colList);
//        }
//    }

//    public LogicalOperation cacheFunc(List<LogicalOperation> baseOperations) {
//        if (baseOperations == null || baseOperations.isEmpty()) {
//            return null;
//        }
//        LogicalOperation baseOperation = null;
//        for (int i = baseOperations.size() - 1; i >= 0; i--) {
//            LogicalOperation baseOperation1 = baseOperations.get(i);
//            if (baseOperation1.getOperationType() == CREATE_TABLE_AS_SQL ||
//                    baseOperation1.getOperationType() == CREATE_SOURCE_TABLE) {
//                baseOperation = baseOperation1;
//                break;
//            }
//        }
//        if (baseOperation != null) {
//            if (baseOperation.getOperationType() == CREATE_TABLE_AS_SQL) {
//                ((LogicalCreateTableAsSQL) baseOperation).setWithCache(true);
//            } else if (baseOperation.getOperationType() == CREATE_SOURCE_TABLE) {
//                ((LogicalCreateSourceTable) baseOperation).setWithCache(true);
//            }
//        }
//        return null;
//    }

//    public LogicalOperation showFunc(PlSqlParser.Function_argumentContext function_argumentContext, List<LogicalOperation> baseOperations) {
//        if (!function_argumentContext.argument().isEmpty() && function_argumentContext.argument().size() != 1) {
//            throw new ParseException("The params of function 'show' must be 0 or 1");
//        }
//        int showCounts = 100;
//        if (function_argumentContext.argument().size() == 1) {
//            try {
//                showCounts = Integer.parseInt(function_argumentContext.argument().get(0).getText());
//            } catch (NumberFormatException e) {
//                throw new ParseException("The params of function 'show' must be INT");
//            }
//        }
//        if (showCounts > 100) {
//            showCounts = 100;
//        }
//        if (baseOperations == null || baseOperations.isEmpty()) {
//            return null;
//        }
//        LogicalOperation baseOperation = null;
//        for (int i = baseOperations.size() - 1; i >= 0; i--) {
//            LogicalOperation baseOperation1 = baseOperations.get(i);
//            if (baseOperation1.getOperationType() == CREATE_TABLE_AS_SQL ||
//                    baseOperation1.getOperationType() == CREATE_SOURCE_TABLE) {
//                baseOperation = baseOperation1;
//                break;
//            }
//        }
//        if (baseOperation != null) {
//            if (baseOperation.getOperationType() == CREATE_TABLE_AS_SQL) {
//                ((LogicalCreateTableAsSQL) baseOperation).setWithShow(showCounts);
//            } else if (baseOperation.getOperationType() == CREATE_SOURCE_TABLE) {
//                ((LogicalCreateSourceTable) baseOperation).setWithShow(showCounts);
//            }
//        }
//        return null;
//    }

}
