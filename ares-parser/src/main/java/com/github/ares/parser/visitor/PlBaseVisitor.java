package com.github.ares.parser.visitor;

import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalAnonymousBody;
import com.github.ares.parser.plan.LogicalDeclareParams;
import com.github.ares.parser.plan.LogicalExceptionHandler;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalSetConfig;
import com.github.ares.parser.utils.PLParserUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PlBaseVisitor {
    private PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public List<LogicalOperation> visitBase(PlSqlParser.Sql_scriptContext sqlScriptContext) {
        List<LogicalOperation> setConfigs = visit4SetConfig(sqlScriptContext);

        List<PlSqlParser.Unit_statementContext> unitStatementContexts = sqlScriptContext.unit_statement();
        if (unitStatementContexts == null) {
            return null;
        }
        List<LogicalOperation> result = new ArrayList<>();
        for (PlSqlParser.Unit_statementContext unitStatementContext : unitStatementContexts) {
            PlSqlParser.Anonymous_bodyContext anonymousBodyContext = unitStatementContext.anonymous_body();
            if (anonymousBodyContext != null) {
                Map<String, PlType> declaredParams = new LinkedHashMap<>();
                LogicalOperation operation = visitorManager.getDeclareParamsVisitor()
                        .visitDeclareParams(unitStatementContext.anonymous_body().seq_of_declare_specs(), declaredParams);
                List<LogicalOperation> body = visitorManager.getBodyVisitor().visitBodyStatements(unitStatementContext.anonymous_body().body().seq_of_statements(), new LinkedHashMap<>(),
                        new LinkedHashMap<>(), declaredParams, result, null);
                LogicalAnonymousBody anonymousBody = new LogicalAnonymousBody();
                anonymousBody.setDeclareParams((LogicalDeclareParams) operation);
                anonymousBody.setAnonymousBody(body);

                if (unitStatementContext.anonymous_body().body().exception_handler() != null &&
                        !unitStatementContext.anonymous_body().body().exception_handler().isEmpty()) {
                    LogicalExceptionHandler exHandler = visitorManager.getExceptionHandlerVisitor()
                            .visitExceptionHandler(unitStatementContext.anonymous_body().body().exception_handler().get(0),
                                    new LinkedHashMap<>(), new LinkedHashMap<>(), declaredParams, false);
                    if (exHandler != null) {
                        anonymousBody.setExHandler(exHandler);
                    }
                }

                result.add(anonymousBody);
                continue;
            }

            PlSqlParser.Create_tableContext createTableContext = unitStatementContext.create_table();
            if (createTableContext != null) {
                PlSqlParser.Create_withContext createWithContext = createTableContext.create_with();
                if (createWithContext != null) {
                    List<LogicalOperation> operations = visitorManager.getCreateTableWithVisitor()
                            .visitCreateTableWith(createTableContext, createWithContext, setConfigs);
                    if (operations != null && !operations.isEmpty()) {
                        result.addAll(operations);
                    }
                    continue;
                } else {
                    throw new UnsupportedOperationException("Unsupported create internal table yet");
                }
            }

            PlSqlParser.Create_procedure_bodyContext createProcedureBody = unitStatementContext.create_procedure_body();
            if (createProcedureBody != null) {
                LogicalOperation operation = visitorManager.getCreateProcedureVisitor().visitCreateProcedure(createProcedureBody, result);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            PlSqlParser.Create_function_bodyContext createFunctionBodyContext = unitStatementContext.create_function_body();
            if (createFunctionBodyContext != null) {
                LogicalOperation operation = visitorManager.getCreateFunctionVisitor().visitCreateFunction(createFunctionBodyContext, result);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            PlSqlParser.Call_statementContext callStatementContext = unitStatementContext.call_statement();
            if (callStatementContext != null) {
                LogicalOperation operation = visitorManager.getCallStatementVisitor().
                        visitCallStatement(callStatementContext, new LinkedHashMap<>(), result, result, null);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            if (unitStatementContext.select_block() != null) {
                String selectSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(unitStatementContext.select_block()));
                LogicalOperation operation = visitorManager.getSelectSQLVisitor()
                        .visitSelectSQL(selectSQL, selectSQL, new LinkedHashMap<>());
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            if (unitStatementContext.insert_block() != null) {
                String insertSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(unitStatementContext.insert_block()));
                LogicalOperation operation = visitorManager.getInsertSQLVisitor().visitInsertSQL(insertSQL, insertSQL);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            if (unitStatementContext.update_block() != null) {
                String updateSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(unitStatementContext.update_block()));
                LogicalOperation operation = visitorManager.getUpdateSQLVisitor().visitUpdateSQL(updateSQL, updateSQL);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            if (unitStatementContext.delete_block() != null) {
                String deleteSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(unitStatementContext.delete_block()));
                LogicalOperation operation = visitorManager.getDeleteSQLVisitor().visitDeleteSQL(deleteSQL, deleteSQL);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            if (unitStatementContext.merge_block() != null) {
                String mergeSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(unitStatementContext.merge_block()));
                LogicalOperation operation = visitorManager.getMergeSQLVisitor().visitMergeSQL(mergeSQL, mergeSQL);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            PlSqlParser.Create_table_asContext createTableAsContext = unitStatementContext.create_table_as();
            if (createTableAsContext != null) {
                String createSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(createTableAsContext));
                String innerTableName = createTableAsContext.table_name().getText();
                LogicalOperation operation = visitorManager.getCreateAsSQLVisitor()
                        .visitCreateInnerTable(createSQL, createSQL, innerTableName);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            if (unitStatementContext.truncate_table_block() != null) {
                String truncateSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(unitStatementContext.truncate_table_block()));
                LogicalOperation operation = visitorManager.getTruncateSQLVisitor().visitTruncateSQL(truncateSQL);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            if (unitStatementContext.set_bleck() != null) {
                continue;
            }

            throw new ParseException(String.format("Unsupported syntax: %s", PLParserUtil.getFullText(unitStatementContext)));
        }

        return result;
    }

    public List<LogicalOperation> visit4SetConfig(PlSqlParser.Sql_scriptContext sqlScriptContext) {
        List<PlSqlParser.Unit_statementContext> unitStatementContexts = sqlScriptContext.unit_statement();
        if (unitStatementContexts == null) {
            return null;
        }
        List<LogicalOperation> setConfigs = new ArrayList<>();
        for (PlSqlParser.Unit_statementContext unitStatementContext : unitStatementContexts) {
            if (unitStatementContext.set_bleck() != null) {
                String setBlock = unitStatementContext.set_bleck().getText();
                setBlock = setBlock.substring(3);
                if (setBlock.endsWith(";")) {
                    setBlock = setBlock.substring(0, setBlock.length() - 1);
                }
                int equalIndex = setBlock.indexOf("=");
                LogicalSetConfig setConfig = new LogicalSetConfig();
                if (equalIndex > -1) {
                    setConfig.setKey(setBlock.substring(0, equalIndex).trim());
                    setConfig.setValue(setBlock.substring(equalIndex + 1).trim());
                } else {
                    setConfig.setKey(setBlock.trim());
                }
                setConfigs.add(setConfig);
            }
        }
        return setConfigs;
    }
}
