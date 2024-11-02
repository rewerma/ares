package com.github.ares.parser.visitor;

import com.github.ares.common.engine.PlType;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalContinueLoop;
import com.github.ares.parser.plan.LogicalExitLoop;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.utils.PLParserUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PlFunctionBodyVisitor extends PlBodyVisitor {

    /**
     * Visits the body statements of a function.
     *
     * @param statementContextList the list of statement contexts
     * @param inParams             input parameters
     * @param outParams            output parameters
     * @param declaredParams       declared parameters
     * @param baseBody             the base body of the function
     * @param structs              the list of structs
     * @return the list of logical operations
     */
    @Override
    public List<LogicalOperation> visitBodyStatements(List<PlSqlParser.StatementContext> statementContextList, Map<String, PlType> inParams,
                                                      Map<String, PlType> outParams, Map<String, PlType> declaredParams,
                                                      List<LogicalOperation> baseBody, List<String> structs) {
        Map<String, PlType> allParams = new LinkedHashMap<>(inParams);
        declaredParams.putAll(outParams);
        allParams.putAll(declaredParams);
        if (statementContextList == null) {
            return Collections.emptyList();
        }
        List<LogicalOperation> result = new ArrayList<>();
        for (PlSqlParser.StatementContext statementContext : statementContextList) {
            if (visitLogicalControlContext(statementContext, result, inParams, declaredParams)
                    || visitPlContext(statementContext, result, allParams, declaredParams, baseBody, structs)) {
                continue;
            }

            throw new UnsupportedOperationException(String.format("Unsupported syntax: '%s' in function body",
                    PLParserUtil.getFullText(statementContext)));
        }
        return result;
    }

    private boolean visitLogicalControlContext(PlSqlParser.StatementContext statementContext, List<LogicalOperation> result,
                                               Map<String, PlType> inParams, Map<String, PlType> declaredParams) {
        boolean resultFlag = false;
        if (statementContext.exit_statement() != null) {
            result.add(new LogicalExitLoop());
            resultFlag = true;
        } else if (statementContext.continue_statement() != null) {
            result.add(new LogicalContinueLoop());
            resultFlag = true;
        } else if (statementContext.return_statement() != null) {
            LogicalOperation operation = visitorManager.getReturnStatementVisitor()
                    .visitReturnStatement(statementContext.return_statement(), inParams, declaredParams);
            if (operation != null) {
                result.add(operation);
            }
            resultFlag = true;
        } else if (statementContext.raise_statement() != null) {
            resultFlag = true;
        }

        return resultFlag;
    }

    private boolean visitPlContext(PlSqlParser.StatementContext statementContext, List<LogicalOperation> result, Map<String, PlType> allParams,
                                   Map<String, PlType> declaredParams, List<LogicalOperation> baseBody, List<String> structs) {
        boolean resultFlag = false;
        if (statementContext.call_statement() != null) {
            LogicalOperation operation = visitorManager.getCallStatementVisitor()
                    .visitCallStatement(statementContext.call_statement(), allParams, result, structs);
            if (operation != null) {
                result.add(operation);
            }
            resultFlag = true;
        } else if (statementContext.assignment_statement() != null) {
            LogicalOperation operation = visitorManager.getAssignmentVisitor()
                    .visitAssignment(statementContext.assignment_statement(), declaredParams, allParams, structs);
            if (operation != null) {
                result.add(operation);
            }
            resultFlag = true;
        } else if (statementContext.if_statement() != null) {
            visitorManager.getIfStatementVisitor().ifElseVisitor(
                    this, statementContext.if_statement(), baseBody, allParams, result, structs
            );
            resultFlag = true;
        } else if (statementContext.loop_statement() != null) {
            LogicalOperation operation = visitorManager.getLoopStatementVisitor().loopVisitor(
                    this, statementContext.loop_statement(), baseBody, allParams, structs, false);
            if (operation != null) {
                result.add(operation);
            }
            resultFlag = true;
        }
        return resultFlag;
    }
}
