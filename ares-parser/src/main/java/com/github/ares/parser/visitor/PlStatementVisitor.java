package com.github.ares.parser.visitor;

import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalProject;
import com.github.ares.parser.plan.LogicalSetConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.github.ares.parser.enums.OperationType.CREATE_FUNCTION;
import static com.github.ares.parser.enums.OperationType.CREATE_PROCEDURE;
import static com.github.ares.parser.enums.OperationType.CREATE_SINK_TABLE;
import static com.github.ares.parser.enums.OperationType.CREATE_SOURCE_TABLE;
import static com.github.ares.parser.enums.OperationType.SET_CONFIG;

public class PlStatementVisitor {

    private PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public LogicalProject visitSqlScriptContext(PlSqlParser.Sql_scriptContext sqlScriptContext) {
        LogicalProject baseBody = new LogicalProject();
        List<LogicalOperation> baseOperations = rebuildOperations(visitorManager.getBaseVisitor().visitBase(sqlScriptContext));
        baseBody.setLogicalOperations(baseOperations);
        return baseBody;
    }

    private List<LogicalOperation> rebuildOperations(List<LogicalOperation> baseOperations) {
        List<LogicalOperation> declareOperations = new ArrayList<>();
        List<LogicalOperation> executionOperations = new ArrayList<>();
        for (LogicalOperation baseOperation : baseOperations) {
             if (baseOperation.getOperationType() == CREATE_SOURCE_TABLE) {
                declareOperations.add(baseOperation);
            } else if (baseOperation.getOperationType() == CREATE_SINK_TABLE) {
                declareOperations.add(baseOperation);
            } else if (baseOperation.getOperationType() == CREATE_PROCEDURE) {
                declareOperations.add(baseOperation);
            } else if (baseOperation.getOperationType() == CREATE_FUNCTION) {
                declareOperations.add(baseOperation);
            } else {
                executionOperations.add(baseOperation);
            }
        }
        declareOperations.addAll(executionOperations);
        return declareOperations;
    }
}
