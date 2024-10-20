package com.github.ares.parser.visitor;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalReturnValue;

import java.util.LinkedHashMap;
import java.util.Map;

public class PlReturnStatementVisitor {

    private PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public LogicalOperation visitReturnStatement(PlSqlParser.Return_statementContext return_statementContext,
                                                 Map<String, PlType> inParams, Map<String, PlType> declaredParams) {
        PlType returnType = declaredParams.get("_return_type_");

        LogicalReturnValue returnValue = new LogicalReturnValue(returnType);
        Map<String, PlType> params = new LinkedHashMap<>(inParams);
        params.putAll(declaredParams);
        returnValue.setExpr(visitorManager.getExpressionVisitor().visitExpressionContext(return_statementContext.expression(), params).getExpr());

        return returnValue;
    }
}
