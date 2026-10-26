package com.github.ares.parser.visitor;

import com.github.ares.common.engine.PlType;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalCallFunction;
import com.github.ares.parser.plan.LogicalExpression;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.utils.PLParserUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PlCallStatementVisitor {
    public void init(PlVisitorManager visitorManager) {}

    public LogicalOperation visitCallStatement(
            PlSqlParser.Call_statementContext callStatement,
            Map<String, PlType> visible,
            List<String> structs) {
        StringBuilder name = new StringBuilder();
        for (PlSqlParser.IdentifierContext identifier : callStatement.identifier()) {
            if (name.length() > 0) {
                name.append('.');
            }
            name.append(PlBodyVisitor.identText(identifier));
        }
        List<LogicalExpression> args = new ArrayList<>();
        if (callStatement.func_args() != null) {
            for (PlSqlParser.Func_argContext arg : callStatement.func_args().func_arg()) {
                LogicalExpression expression = new LogicalExpression();
                expression.setExpr(PLParserUtil.getFullExprWithParams(arg, visible, structs));
                args.add(expression);
            }
        }
        LogicalCallFunction callFunction = new LogicalCallFunction();
        callFunction.setFuncName(name.toString());
        callFunction.setArgs(args);
        return callFunction;
    }
}
