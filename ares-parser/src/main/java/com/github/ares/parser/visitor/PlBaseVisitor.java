package com.github.ares.parser.visitor;

import com.github.ares.common.engine.PlType;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalAnonymousBody;
import com.github.ares.parser.plan.LogicalDeclareParams;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalSetConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PlBaseVisitor {
    private PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    /**
     * Visit a script. Catalog statements (SET, CREATE TABLE ... USING) stay on the project list.
     * Everything else shares one body so variables and control flow run with the same parameters.
     */
    public List<LogicalOperation> visitBase(PlSqlParser.Sql_scriptContext sqlScriptContext) {
        List<PlSqlParser.StatementContext> statements = sqlScriptContext.statement();
        if (statements == null || statements.isEmpty()) {
            return Collections.emptyList();
        }
        List<LogicalSetConfig> setConfigs = new ArrayList<>();
        for (PlSqlParser.StatementContext statement : statements) {
            if (statement.terminated_statement() != null
                    && statement.terminated_statement().set_statement() != null) {
                setConfigs.add(
                        PlBodyVisitor.toSetConfig(
                                statement.terminated_statement().set_statement()));
            }
        }
        visitorManager.getCreateTableWithVisitor().setSetConfigs(setConfigs);

        Map<String, PlType> declared = new LinkedHashMap<>();
        List<LogicalOperation> project = new ArrayList<>();
        List<LogicalOperation> body = new ArrayList<>();
        for (PlSqlParser.StatementContext statement : statements) {
            List<LogicalOperation> produced =
                    visitorManager
                            .getBodyVisitor()
                            .visitStatement(statement, declared, declared, null, true);
            if (PlBodyVisitor.containsCatalogTable(produced)) {
                project.addAll(produced);
            } else {
                body.addAll(produced);
            }
        }
        if (!body.isEmpty() || !declared.isEmpty()) {
            LogicalAnonymousBody anonymousBody = new LogicalAnonymousBody();
            anonymousBody.setDeclareParams(toDeclare(declared));
            anonymousBody.setAnonymousBody(body);
            project.add(anonymousBody);
        }
        return project;
    }

    private static LogicalDeclareParams toDeclare(Map<String, PlType> declared) {
        if (declared.isEmpty()) {
            return null;
        }
        List<Argument> arguments = new ArrayList<>();
        declared.forEach(
                (name, type) -> {
                    Argument argument = new Argument(name, type);
                    argument.setDefaultVal("null");
                    arguments.add(argument);
                });
        LogicalDeclareParams declareParams = new LogicalDeclareParams();
        declareParams.setDeclareParams(arguments);
        return declareParams;
    }
}
