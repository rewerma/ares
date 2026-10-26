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

    public LogicalOperation visitAssignment(
            PlSqlParser.Assignment_statementContext assignmentStatement,
            Map<String, PlType> visible,
            List<String> structs) {
        StringBuilder name = new StringBuilder();
        for (PlSqlParser.IdentifierContext identifier : assignmentStatement.identifier()) {
            if (name.length() > 0) {
                name.append('.');
            }
            name.append(PlBodyVisitor.identText(identifier));
        }
        String element = name.toString();
        PlType type = visible.get(element);
        if (type == null) {
            throw new IllegalArgumentException("Argument: " + element + " undefined.");
        }
        LogicalAssignment assignment = new LogicalAssignment();
        assignment.setParam(new Argument(element, type));
        assignment.setExpr(
                visitorManager
                        .getExpressionVisitor()
                        .visitExpressionContext(assignmentStatement.expression(), visible, structs)
                        .getExpr());
        return assignment;
    }
}
