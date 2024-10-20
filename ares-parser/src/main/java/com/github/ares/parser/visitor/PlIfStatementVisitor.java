package com.github.ares.parser.visitor;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.engine.PlType;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalExpression;
import com.github.ares.parser.plan.LogicalIfElse;
import com.github.ares.parser.plan.LogicalOperation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PlIfStatementVisitor {
    private PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public void ifElseVisitor(PlBodyVisitor plBodyVisitor, PlSqlParser.If_statementContext ifStatement,
                              List<LogicalOperation> baseBody, Map<String, PlType> allParams,
                              List<LogicalOperation> result, List<String> structs) {
        LogicalOperation operation = generateIfOperation("if",
                visitorManager.getExpressionVisitor().visitExpressionContext(ifStatement.condition().expression(), allParams, structs),
                plBodyVisitor.visitBodyStatements(ifStatement.seq_of_statements(), new LinkedHashMap<>(), new LinkedHashMap<>(),
                        allParams, baseBody, structs));
        result.add(operation);

        List<PlSqlParser.Elsif_partContext> elseif = ifStatement.elsif_part();
        for (PlSqlParser.Elsif_partContext elsif_part : elseif) {
            LogicalOperation operation2 = generateIfOperation("else if",
                    visitorManager.getExpressionVisitor().visitExpressionContext(elsif_part.condition().expression(), allParams, structs),
                    plBodyVisitor.visitBodyStatements(elsif_part.seq_of_statements(), new LinkedHashMap<>(), new LinkedHashMap<>(),
                            allParams, baseBody, structs));
            result.add(operation2);
        }
        if (ifStatement.else_part() != null) {
            LogicalOperation operation2 = generateIfOperation("else", null,
                    plBodyVisitor.visitBodyStatements(ifStatement.else_part().seq_of_statements(), new LinkedHashMap<>(), new LinkedHashMap<>(),
                            allParams, baseBody, structs));
            result.add(operation2);
        }
    }

    private LogicalOperation generateIfOperation(String type, LogicalExpression confExpr, List<LogicalOperation> ifBody) {
        LogicalIfElse ifElse = new LogicalIfElse();
        if ("if".equalsIgnoreCase(type)) {
            ifElse.setIsIf(true);
        } else if ("else if".equalsIgnoreCase(type)) {
            ifElse.setIsElseIf(true);
        } else if ("else".equalsIgnoreCase(type)) {
            ifElse.setIsElse(true);
        }
        if (confExpr != null) {
            ifElse.setCondition(confExpr);
        }
        ifElse.setIfBody(ifBody);
//        translation.ifElseOp(ifElse);
        return ifElse;
    }
}
