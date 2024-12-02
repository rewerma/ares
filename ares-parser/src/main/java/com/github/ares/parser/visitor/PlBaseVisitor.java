package com.github.ares.parser.visitor;

import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalAnonymousBody;
import com.github.ares.parser.plan.LogicalDeclareParams;
import com.github.ares.parser.plan.LogicalExceptionHandler;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalSetConfig;
import com.github.ares.parser.plan.LogicalSetConfigs;
import com.github.ares.parser.utils.PLParserUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.ares.parser.enums.OperationType.SET_CONFIGS;

public class PlBaseVisitor {
    private PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    /**
     * visit PL script and return a list of logical operations
     *
     * @param sqlScriptContext PL SQL script context object
     * @return a list of logical operations
     */
    public List<LogicalOperation> visitBase(PlSqlParser.Sql_scriptContext sqlScriptContext) {
        List<PlSqlParser.Unit_statementContext> unitStatementContexts = sqlScriptContext.unit_statement();
        if (unitStatementContexts == null) {
            return Collections.emptyList();
        }
        List<LogicalOperation> result = new ArrayList<>();
        visitSetConfigs(sqlScriptContext, result);
        for (PlSqlParser.Unit_statementContext unitStatementContext : unitStatementContexts) {
            if (visitPlContext(unitStatementContext, result) ||
                    visitSqlContext(unitStatementContext, result)) {
                continue;
            }
            throw new ParseException(String.format("Unsupported syntax: %s", PLParserUtil.getFullText(unitStatementContext)));
        }

        return result;
    }

    private void visitAnonymousBody(PlSqlParser.Anonymous_bodyContext anonymousBodyContext, List<LogicalOperation> result) {
        Map<String, PlType> declaredParams = new LinkedHashMap<>();
        LogicalOperation operation = visitorManager.getDeclareParamsVisitor()
                .visitDeclareParams(anonymousBodyContext.seq_of_declare_specs(), declaredParams);
        List<LogicalOperation> body = visitorManager.getBodyVisitor().visitBodyStatements(anonymousBodyContext.body().seq_of_statements(), new LinkedHashMap<>(),
                new LinkedHashMap<>(), declaredParams, result, null);
        LogicalAnonymousBody anonymousBody = new LogicalAnonymousBody();
        anonymousBody.setDeclareParams((LogicalDeclareParams) operation);
        anonymousBody.setAnonymousBody(body);

        if (anonymousBodyContext.body().exception_handler() != null &&
                !anonymousBodyContext.body().exception_handler().isEmpty()) {
            LogicalExceptionHandler exHandler = visitorManager.getExceptionHandlerVisitor()
                    .visitExceptionHandler(anonymousBodyContext.body().exception_handler().get(0),
                            new LinkedHashMap<>(), new LinkedHashMap<>(), declaredParams, false);
            if (exHandler != null) {
                anonymousBody.setExHandler(exHandler);
            }
        }

        result.add(anonymousBody);
    }

    private void visitCreateWithSQL(PlSqlParser.Create_tableContext createTableContext, List<LogicalOperation> result) {
        List<LogicalOperation> setConfigs;
        Optional<LogicalOperation> logicalSetConfigsOp = result.stream()
                .filter(op -> op.getOperationType() == SET_CONFIGS).findFirst();
        if (logicalSetConfigsOp.isPresent()) {
            LogicalSetConfigs logicalSetConfigs = (LogicalSetConfigs) logicalSetConfigsOp.get();
            setConfigs = logicalSetConfigs.getLogicalSetConfigs().stream()
                    .map(v -> (LogicalOperation) v).collect(Collectors.toList());
        } else {
            setConfigs = new ArrayList<>();
        }

        PlSqlParser.Create_withContext createWithContext = createTableContext.create_with();
        if (createWithContext != null) {
            List<LogicalOperation> operations = visitorManager.getCreateTableWithVisitor()
                    .visitCreateTableWith(createTableContext, createWithContext, setConfigs);
            if (operations != null && !operations.isEmpty()) {
                result.addAll(operations);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported create internal table yet");
        }
    }

    private void visitCreateProcedure(PlSqlParser.Create_procedure_bodyContext createProcedureBody, List<LogicalOperation> result) {
        LogicalOperation operation = visitorManager.getCreateProcedureVisitor().visitCreateProcedure(createProcedureBody, result);
        if (operation != null) {
            result.add(operation);
        }
    }

    private void visitCreateFunction(PlSqlParser.Create_function_bodyContext createFunctionBodyContext, List<LogicalOperation> result) {
        LogicalOperation operation = visitorManager.getCreateFunctionVisitor().visitCreateFunction(createFunctionBodyContext, result);
        if (operation != null) {
            result.add(operation);
        }
    }

    private void visitCallStatement(PlSqlParser.Call_statementContext callStatementContext, List<LogicalOperation> result) {
        LogicalOperation operation = visitorManager.getCallStatementVisitor().
                visitCallStatement(callStatementContext, new LinkedHashMap<>(), result, null);
        if (operation != null) {
            result.add(operation);
        }
    }

    private void visitSelectSQL(PlSqlParser.Select_blockContext selectBlockContext, List<LogicalOperation> result) {
        String selectSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(selectBlockContext));
        LogicalOperation operation = visitorManager.getSelectSQLVisitor()
                .visitSelectSQL(selectSQL, selectSQL, new LinkedHashMap<>());
        if (operation != null) {
            result.add(operation);
        }
    }

    private void visitInsertSQL(PlSqlParser.Insert_blockContext insertBlockContext, List<LogicalOperation> result) {
        String insertSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(insertBlockContext));
        LogicalOperation operation = visitorManager.getInsertSQLVisitor().visitInsertSQL(insertSQL, insertSQL);
        if (operation != null) {
            result.add(operation);
        }
    }

    private void visitUpdateSQL(PlSqlParser.Update_blockContext updateBlockContext, List<LogicalOperation> result) {
        String updateSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(updateBlockContext));
        LogicalOperation operation = visitorManager.getUpdateSQLVisitor().visitUpdateSQL(updateSQL, updateSQL);
        if (operation != null) {
            result.add(operation);
        }
    }

    private void visitDeleteSQL(PlSqlParser.Delete_blockContext deleteBlockContext, List<LogicalOperation> result) {
        String deleteSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(deleteBlockContext));
        LogicalOperation operation = visitorManager.getDeleteSQLVisitor().visitDeleteSQL(deleteSQL, deleteSQL);
        if (operation != null) {
            result.add(operation);
        }
    }

    private void visitMergeSQL(PlSqlParser.Merge_blockContext mergeBlockContext, List<LogicalOperation> result) {
        String mergeSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(mergeBlockContext));
        LogicalOperation operation = visitorManager.getMergeSQLVisitor().visitMergeSQL(mergeSQL, mergeSQL);
        if (operation != null) {
            result.add(operation);
        }
    }

    private void visitCreateTableAsSQL(PlSqlParser.Create_table_asContext createTableAsContext, List<LogicalOperation> result) {
        String createSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(createTableAsContext));
        String innerTableName = createTableAsContext.table_name().getText();
        LogicalOperation operation = visitorManager.getCreateAsSQLVisitor()
                .visitCreateTableAsSQL(createSQL, createSQL, innerTableName);
        if (operation != null) {
            result.add(operation);
        }
    }

    private void visitTruncateTable(PlSqlParser.Truncate_table_blockContext truncateTableBlockContext, List<LogicalOperation> result) {
        String truncateSQL = PLParserUtil.cleanSQL(PLParserUtil.getFullText(truncateTableBlockContext));
        LogicalOperation operation = visitorManager.getTruncateSQLVisitor().visitTruncateSQL(truncateSQL);
        if (operation != null) {
            result.add(operation);
        }
    }

    public List<LogicalSetConfig> visit4SetConfig(PlSqlParser.Sql_scriptContext sqlScriptContext) {
        List<PlSqlParser.Unit_statementContext> unitStatementContexts = sqlScriptContext.unit_statement();
        if (unitStatementContexts == null) {
            return Collections.emptyList();
        }
        List<LogicalSetConfig> setConfigs = new ArrayList<>();
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

    private void visitSetConfigs(PlSqlParser.Sql_scriptContext sqlScriptContext, List<LogicalOperation> result) {
        if (sqlScriptContext.unit_statement() == null) {
            return;
        }
        List<LogicalSetConfig> setConfigs = visit4SetConfig(sqlScriptContext);
        LogicalSetConfigs logicalSetConfigs = new LogicalSetConfigs();
        logicalSetConfigs.setLogicalSetConfigs(setConfigs);
        result.add(logicalSetConfigs);
    }

    private boolean visitPlContext(PlSqlParser.Unit_statementContext unitStatementContext, List<LogicalOperation> result) {
        boolean resultFlag = false;
        if (unitStatementContext.anonymous_body() != null) {
            visitAnonymousBody(unitStatementContext.anonymous_body(), result);
            resultFlag = true;
        } else if (unitStatementContext.create_table() != null) {
            visitCreateWithSQL(unitStatementContext.create_table(), result);
            resultFlag = true;
        } else if (unitStatementContext.create_procedure_body() != null) {
            visitCreateProcedure(unitStatementContext.create_procedure_body(), result);
            resultFlag = true;
        } else if (unitStatementContext.create_function_body() != null) {
            visitCreateFunction(unitStatementContext.create_function_body(), result);
            resultFlag = true;
        } else if (unitStatementContext.call_statement() != null) {
            visitCallStatement(unitStatementContext.call_statement(), result);
            resultFlag = true;
        } else if (unitStatementContext.set_bleck() != null) {
            resultFlag = true;
        }
        return resultFlag;
    }

    private boolean visitSqlContext(PlSqlParser.Unit_statementContext unitStatementContext, List<LogicalOperation> result) {
        boolean resultFlag = false;
        if (unitStatementContext.select_block() != null) {
            visitSelectSQL(unitStatementContext.select_block(), result);
            resultFlag = true;
        } else if (unitStatementContext.insert_block() != null) {
            visitInsertSQL(unitStatementContext.insert_block(), result);
            resultFlag = true;
        } else if (unitStatementContext.update_block() != null) {
            visitUpdateSQL(unitStatementContext.update_block(), result);
            resultFlag = true;
        } else if (unitStatementContext.delete_block() != null) {
            visitDeleteSQL(unitStatementContext.delete_block(), result);
            resultFlag = true;
        } else if (unitStatementContext.merge_block() != null) {
            visitMergeSQL(unitStatementContext.merge_block(), result);
            resultFlag = true;
        } else if (unitStatementContext.create_table_as() != null) {
            visitCreateTableAsSQL(unitStatementContext.create_table_as(), result);
            resultFlag = true;
        } else if (unitStatementContext.truncate_table_block() != null) {
            visitTruncateTable(unitStatementContext.truncate_table_block(), result);
            resultFlag = true;
        }
        return resultFlag;
    }
}
