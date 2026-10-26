package com.github.ares.parser.visitor;

import com.github.ares.common.engine.PlType;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PlExceptionHandlerVisitor {
    private static final String INNER_EX_PARAM = "ex";

    public void init(PlVisitorManager visitorManager) {}

    public LogicalExceptionHandler visitTry(
            PlBodyVisitor plBodyVisitor,
            PlSqlParser.Try_statementContext tryStatement,
            Map<String, PlType> scriptDeclared,
            Map<String, PlType> visible,
            List<String> structs) {
        LogicalExceptionHandler handler = new LogicalExceptionHandler();
        handler.setTryBody(
                plBodyVisitor.visitBlock(tryStatement.body(0), scriptDeclared, visible, structs));

        List<String> catchStructs = structs == null ? new ArrayList<>() : new ArrayList<>(structs);
        if (!catchStructs.contains(INNER_EX_PARAM)) {
            catchStructs.add(INNER_EX_PARAM);
        }
        PlSqlParser.BodyContext catchBlock = tryStatement.body(1);
        List<PlSqlParser.StatementContext> statements =
                catchBlock.statement() == null
                        ? new ArrayList<>()
                        : new ArrayList<>(catchBlock.statement());
        for (int i = 0; i < statements.size(); i++) {
            PlSqlParser.Terminated_statementContext terminated =
                    statements.get(i).terminated_statement();
            if (terminated != null && terminated.raise_statement() != null) {
                handler.setWithRaise(true);
                statements = statements.subList(0, i);
                break;
            }
        }
        List<com.github.ares.parser.plan.LogicalOperation> catchBody = new ArrayList<>();
        for (PlSqlParser.StatementContext statement : statements) {
            catchBody.addAll(
                    plBodyVisitor.visitStatement(
                            statement, scriptDeclared, visible, catchStructs, false));
        }
        handler.setExHandlerBody(catchBody);
        return handler;
    }
}
