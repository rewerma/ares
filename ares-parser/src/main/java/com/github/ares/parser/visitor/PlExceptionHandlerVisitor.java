package com.github.ares.parser.visitor;

import com.github.ares.common.engine.PlType;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalExceptionHandler;
import com.github.ares.parser.plan.LogicalOperation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PlExceptionHandlerVisitor {
    private static final String INNER_EX_PARAM = "ex";
    private PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public LogicalExceptionHandler visitExceptionHandler(PlSqlParser.Exception_handlerContext exceptionHandlerContext,
                                                         Map<String, PlType> inParams, Map<String, PlType> outParams, Map<String, PlType> declaredParams,
                                                         boolean inFunction) {
        String exName = exceptionHandlerContext.exception_name(0).getText();
        if (!INNER_EX_PARAM.equalsIgnoreCase(exName)) {
            return null;
        }
        LogicalExceptionHandler exHandler = new LogicalExceptionHandler();
        Integer raiseIdx = null;
        List<PlSqlParser.StatementContext> statements = exceptionHandlerContext.seq_of_statements().statement();
        for (int i = statements.size() - 1; i >= 0; i--) {
            PlSqlParser.Raise_statementContext raiseStatementContext = statements.get(i).raise_statement();
            if (raiseStatementContext != null) {
                exHandler.setWithRaise(true);
                raiseIdx = i;
                break;
            }
        }
        if (raiseIdx != null) {
            List<PlSqlParser.StatementContext> newStatements = new ArrayList<>();
            for (int i = 0; i <= raiseIdx; i++) {
                newStatements.add(statements.get(i));
            }
            statements = newStatements;
        }
        List<LogicalOperation> exHandlerBody;
        if (inFunction) {
            exHandlerBody = visitorManager.getFunctionBodyVisitor().visitBodyStatements(statements, inParams, outParams,
                    declaredParams, new ArrayList<>(), Collections.singletonList(INNER_EX_PARAM));
        } else {
            exHandlerBody = visitorManager.getBodyVisitor().visitBodyStatements(statements, inParams, outParams,
                    declaredParams, new ArrayList<>(), Collections.singletonList(INNER_EX_PARAM));
        }
        exHandler.setExHandlerBody(exHandlerBody);

        return exHandler;
    }
}
