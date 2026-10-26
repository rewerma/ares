package com.github.ares.parser.visitor;

import com.github.ares.common.engine.PlType;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalExpression;
import com.github.ares.parser.plan.LogicalIfElse;
import com.github.ares.parser.plan.LogicalOperation;
import java.util.ArrayList;
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
            Map<String, PlType> scriptDeclared,
            Map<String, PlType> visible,
            List<LogicalOperation> result,
            List<String> structs) {
        LogicalIfElse ifElse =
                newBranch(
                        expression(ifStatement.expression(), visible, structs),
                        plBodyVisitor.visitBlock(
                                ifStatement.body(), scriptDeclared, visible, structs));

        List<LogicalIfElse> elseIfs = new ArrayList<>();
        if (ifStatement.elsif_clause() != null) {
            for (PlSqlParser.Elsif_clauseContext elsif : ifStatement.elsif_clause()) {
                elseIfs.add(
                        newBranch(
                                expression(elsif.expression(), visible, structs),
                                plBodyVisitor.visitBlock(
                                        elsif.body(), scriptDeclared, visible, structs)));
            }
        }
        ifElse.setElseIfs(elseIfs);

        if (ifStatement.else_clause() != null) {
            ifElse.setElseBody(
                    plBodyVisitor.visitBlock(
                            ifStatement.else_clause().body(), scriptDeclared, visible, structs));
        }
        result.add(ifElse);
    }

    private LogicalExpression expression(
            PlSqlParser.ExpressionContext expression,
            Map<String, PlType> visible,
            List<String> structs) {
        return visitorManager
                .getExpressionVisitor()
                .visitExpressionContext(expression, visible, structs);
    }

    private LogicalIfElse newBranch(LogicalExpression condition, List<LogicalOperation> body) {
        LogicalIfElse branch = new LogicalIfElse();
        branch.setCondition(condition);
        branch.setIfBody(body);
        return branch;
    }
}
