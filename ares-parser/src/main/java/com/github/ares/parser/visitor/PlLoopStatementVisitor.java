package com.github.ares.parser.visitor;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalExpression;
import com.github.ares.parser.plan.LogicalForCursorLoop;
import com.github.ares.parser.plan.LogicalForLoop;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalWhileLoop;
import com.github.ares.parser.utils.PLParserUtil;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PlLoopStatementVisitor {

    private PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public LogicalOperation visitWhile(
            PlBodyVisitor plBodyVisitor,
            PlSqlParser.While_statementContext whileStatement,
            Map<String, PlType> scriptDeclared,
            Map<String, PlType> visible,
            List<String> structs) {
        LogicalWhileLoop whileLoop = new LogicalWhileLoop();
        whileLoop.setCondition(
                visitorManager
                        .getExpressionVisitor()
                        .visitExpressionContext(whileStatement.expression(), visible, structs));
        whileLoop.setWhileBody(
                plBodyVisitor.visitBlock(whileStatement.body(), scriptDeclared, visible, structs));
        return whileLoop;
    }

    public LogicalOperation visitFor(
            PlBodyVisitor plBodyVisitor,
            PlSqlParser.For_statementContext forStatement,
            Map<String, PlType> scriptDeclared,
            Map<String, PlType> visible,
            List<String> structs) {
        String name = PlBodyVisitor.identText(forStatement.identifier());
        PlSqlParser.For_sourceContext source = forStatement.for_source();
        if (source.for_query() != null) {
            String selectSQL =
                    PLParserUtil.getFullSQLWithParams(source.for_query(), visible, structs).trim();
            if (selectSQL.startsWith("(") && selectSQL.endsWith(")")) {
                selectSQL = selectSQL.substring(1, selectSQL.length() - 1).trim();
            }
            List<String> bodyStructs =
                    structs == null ? new ArrayList<>() : new ArrayList<>(structs);
            bodyStructs.add(name);
            LogicalForCursorLoop forCursorLoop = new LogicalForCursorLoop();
            forCursorLoop.setCursorName(name);
            forCursorLoop.setSelectSQL(selectSQL);
            forCursorLoop.setForBody(
                    plBodyVisitor.visitBlock(
                            forStatement.body(), scriptDeclared, visible, bodyStructs));
            return forCursorLoop;
        }
        Map<String, PlType> loopVisible = new LinkedHashMap<>(visible);
        loopVisible.put(name, PlType.of(InternalFieldType.INT));
        LogicalForLoop forLoop = new LogicalForLoop();
        forLoop.setIndexName(name);
        forLoop.setLowerExpr(expression(source.expression(0), visible, structs));
        forLoop.setUpperExpr(expression(source.expression(1), visible, structs));
        forLoop.setForBody(
                plBodyVisitor.visitBlock(
                        forStatement.body(), scriptDeclared, loopVisible, structs));
        return forLoop;
    }

    private LogicalExpression expression(
            PlSqlParser.ExpressionContext expression,
            Map<String, PlType> visible,
            List<String> structs) {
        return visitorManager
                .getExpressionVisitor()
                .visitExpressionContext(expression, visible, structs);
    }
}
