package com.github.ares.parser.visitor;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.parser.plan.LogicalExitLoop;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.utils.PLParserUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PlFunctionBodyVisitor extends PlBodyVisitor {

    public List<LogicalOperation> visitBodyStatements(List<PlSqlParser.StatementContext> statementContextList, Map<String, PlType> inParams,
                                                      Map<String, PlType> outParams, Map<String, PlType> declaredParams,
                                                      List<LogicalOperation> baseBody, List<String> structs) {
        Map<String, PlType> allParams = new LinkedHashMap<>(inParams);
        declaredParams.putAll(outParams);
        allParams.putAll(declaredParams);
        if (statementContextList == null) {
            return null;
        }
        List<LogicalOperation> result = new ArrayList<>();
        for (PlSqlParser.StatementContext statementContext : statementContextList) {
            PlSqlParser.Exit_statementContext exit_statementContext = statementContext.exit_statement();
            if (exit_statementContext != null) {
                result.add(new LogicalExitLoop());
                continue;
            }

            PlSqlParser.Call_statementContext callStatementContext = statementContext.call_statement();
            if (callStatementContext != null) {
                LogicalOperation operation = visitorManager.getCallStatementVisitor()
                        .visitCallStatement(callStatementContext, allParams, baseBody, result, structs);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            PlSqlParser.Assignment_statementContext assignmentStatement = statementContext.assignment_statement();
            if (assignmentStatement != null) {
                LogicalOperation operation = visitorManager.getAssignmentVisitor()
                        .visitAssignment(assignmentStatement, declaredParams, allParams, structs);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            PlSqlParser.If_statementContext ifStatement = statementContext.if_statement();
            if (ifStatement != null) {
                visitorManager.getIfStatementVisitor().ifElseVisitor(
                        this, ifStatement, baseBody, allParams, result, structs
                );
                continue;
            }

            PlSqlParser.Loop_statementContext loopStatementContext = statementContext.loop_statement();
            if (loopStatementContext != null) {
                LogicalOperation operation = visitorManager.getLoopStatementVisitor().loopVisitor(
                        this, loopStatementContext, baseBody, allParams, structs, false);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            PlSqlParser.Return_statementContext return_statementContext = statementContext.return_statement();
            if (return_statementContext != null) {
                LogicalOperation operation = visitorManager.getReturnStatementVisitor()
                        .visitReturnStatement(return_statementContext, inParams, declaredParams);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            PlSqlParser.Raise_statementContext raiseStatementContext = statementContext.raise_statement();
            if (raiseStatementContext != null) {
                continue;
            }

            throw new UnsupportedOperationException(String.format("Unsupported syntax: '%s' in function body",
                    PLParserUtil.getFullText(statementContext)));
        }
        return result;
    }
}
