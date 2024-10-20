package com.github.ares.parser.visitor;

import com.github.ares.common.engine.PlType;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalAssignment;
import com.github.ares.parser.plan.LogicalOperation;

import java.util.List;
import java.util.Map;

public class PlAssignmentVisitor {
    private PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public LogicalOperation visitAssignment(PlSqlParser.Assignment_statementContext assignmentStatement,
                                            Map<String, PlType> declaredParams, Map<String, PlType> allParams,
                                            List<String> structs) {
        String element = assignmentStatement.general_element().getText();
        PlType type = declaredParams.get(element);
        if (type == null) {
            throw new IllegalArgumentException("Argument: " + element + " undefined.");
        }
        LogicalAssignment assignment = new LogicalAssignment();
        Argument argument = new Argument(element, type);
        argument.setName(element);
        assignment.setParam(argument);
        assignment.setExpr(visitorManager.getExpressionVisitor().visitExpressionContext(assignmentStatement.expression(), allParams, structs).getExpr());
        return assignment;
    }
}
