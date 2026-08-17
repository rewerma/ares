package com.github.ares.parser.visitor;

import com.github.ares.common.engine.PlType;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalExpression;
import com.github.ares.parser.plan.LogicalIfElse;
import com.github.ares.parser.plan.LogicalOperation;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PlIfStatementVisitor {
    private PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public void ifElseVisitor(
            PlBodyVisitor plBodyVisitor,
            PlSqlParser.If_statementContext ifStatement,
            List<LogicalOperation> baseBody,
            Map<String, PlType> allParams,
            List<LogicalOperation> result,
            List<String> structs) {
        LogicalIfElse ifElse =
                newBranch(
                        visitorManager
                                .getExpressionVisitor()
                                .visitExpressionContext(
                                        ifStatement.condition().expression(), allParams, structs),
                        plBodyVisitor.visitBodyStatements(
                                ifStatement.seq_of_statements(),
                                new LinkedHashMap<>(),
                                new LinkedHashMap<>(),
                                allParams,
                                baseBody,
                                structs));

        List<LogicalIfElse> elseIfs = new ArrayList<>();
        List<PlSqlParser.Elsif_partContext> elseif = ifStatement.elsif_part();
        if (elseif != null) {
            for (PlSqlParser.Elsif_partContext elsifPart : elseif) {
                elseIfs.add(
                        newBranch(
                                visitorManager
                                        .getExpressionVisitor()
                                        .visitExpressionContext(
                                                elsifPart.condition().expression(),
                                                allParams,
                                                structs),
                                plBodyVisitor.visitBodyStatements(
                                        elsifPart.seq_of_statements(),
                                        new LinkedHashMap<>(),
                                        new LinkedHashMap<>(),
                                        allParams,
                                        baseBody,
                                        structs)));
            }
        }
        ifElse.setElseIfs(elseIfs);

        if (ifStatement.else_part() != null) {
            ifElse.setElseBody(
                    plBodyVisitor.visitBodyStatements(
                            ifStatement.else_part().seq_of_statements(),
                            new LinkedHashMap<>(),
                            new LinkedHashMap<>(),
                            allParams,
                            baseBody,
                            structs));
        }
        result.add(ifElse);
    }

    private LogicalIfElse newBranch(LogicalExpression condition, List<LogicalOperation> body) {
        LogicalIfElse branch = new LogicalIfElse();
        branch.setCondition(condition);
        branch.setIfBody(body);
        return branch;
    }
}
