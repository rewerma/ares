package com.github.ares.parser.visitor;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.parser.plan.LogicalExpression;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.utils.PLParserUtil;

import java.util.List;
import java.util.Map;

public class PlExpressionVisitor {

    public LogicalExpression visitExpressionContext(PlSqlParser.ExpressionContext expressionContext, Map<String, PlType> params, List<String> structs) {
        String expressionStr = PLParserUtil.getFullExprWithParams(expressionContext, params, structs);
        return generateExpression(expressionStr);
    }

    public LogicalExpression visitExpressionContext(PlSqlParser.ExpressionContext expressionContext, Map<String, PlType> params) {
        String expressionStr = PLParserUtil.getFullExprWithParams(expressionContext, params,null);
        return generateExpression(expressionStr);
    }

    public LogicalExpression visitExpressionContext(PlSqlParser.ConcatenationContext concatenationContext, Map<String, PlType> params, List<String> struct) {
        String expressionStr = PLParserUtil.getFullExprWithParams(concatenationContext, params, struct);
        return generateExpression(expressionStr);
    }

    private LogicalExpression generateExpression(String expr) {
        LogicalExpression expression = new LogicalExpression();
        expression.setExpr(expr);
        return expression;
    }
}
