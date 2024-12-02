package com.github.ares.parser.visitor;

import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalContinueLoop;
import com.github.ares.parser.plan.LogicalExitLoop;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.utils.PLParserUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PlBodyVisitor {
    private static final int SQL_PREFIX_LEN = 6;

    protected PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    /**
     * Visits a list of statements in a PL/SQL block.
     *
     * @param statementContextList List of statement contexts.
     * @param inParams             input parameters
     * @param outParams            output parameters
     * @param declaredParams       declared parameters
     * @param baseBody             base body
     * @param structs              structs
     * @return List of logical operations.
     */
    public List<LogicalOperation> visitBodyStatements(
            List<PlSqlParser.StatementContext> statementContextList,
            Map<String, PlType> inParams,
            Map<String, PlType> outParams,
            Map<String, PlType> declaredParams,
            List<LogicalOperation> baseBody,
            List<String> structs) {
        Map<String, PlType> allParams = new LinkedHashMap<>(inParams);
        declaredParams.putAll(outParams);
        allParams.putAll(declaredParams);
        if (statementContextList == null) {
            return Collections.emptyList();
        }
        List<LogicalOperation> result = new ArrayList<>();
        for (PlSqlParser.StatementContext statementContext : statementContextList) {
            if (visitLogicalControlContext(statementContext, result)
                    || visitPlContext(statementContext, result, allParams, baseBody, declaredParams, structs)) {
                continue;
            }

            throw new UnsupportedOperationException(String.format("Unsupported syntax: '%s' in body",
                    PLParserUtil.getFullText(statementContext)));
        }
        return result;
    }

    public List<LogicalOperation> visitBodyStatements(
            PlSqlParser.Seq_of_statementsContext seq_of_statementsContext,
            Map<String, PlType> inParams,
            Map<String, PlType> outParams,
            Map<String, PlType> declaredParams,
            List<LogicalOperation> baseBody,
            List<String> structs) {
        return visitBodyStatements(seq_of_statementsContext.statement(), inParams, outParams, declaredParams, baseBody, structs);
    }

    private boolean visitLogicalControlContext(PlSqlParser.StatementContext statementContext, List<LogicalOperation> result) {
        boolean resultFlag = false;
        if (statementContext.exit_statement() != null) {
            result.add(new LogicalExitLoop());
            resultFlag = true;
        } else if (statementContext.continue_statement() != null) {
            result.add(new LogicalContinueLoop());
            resultFlag = true;
        } else if (statementContext.raise_statement() != null) {
            resultFlag = true;
        }
        return resultFlag;
    }

    private boolean visitPlContext(
            PlSqlParser.StatementContext statementContext,
            List<LogicalOperation> result,
            Map<String, PlType> allParams,
            List<LogicalOperation> baseBody,
            Map<String, PlType> declaredParams,
            List<String> structs) {
        boolean resultFlag = false;
        if (statementContext.call_statement() != null) {
            LogicalOperation operation = visitorManager.getCallStatementVisitor()
                    .visitCallStatement(statementContext.call_statement(), allParams, result, structs);
            if (operation != null) {
                result.add(operation);
            }
            resultFlag = true;
        } else if (statementContext.sql_statement() != null) {
            LogicalOperation operation = sqlStatementVisitor(statementContext.sql_statement(), declaredParams, allParams, structs);
            if (operation != null) {
                result.add(operation);
            }
            resultFlag = true;
        } else if (statementContext.assignment_statement() != null) {
            LogicalOperation operation = visitorManager.getAssignmentVisitor()
                    .visitAssignment(statementContext.assignment_statement(), declaredParams, allParams, structs);
            if (operation != null) {
                result.add(operation);
            }
            resultFlag = true;
        } else if (statementContext.if_statement() != null) {
            visitorManager.getIfStatementVisitor().ifElseVisitor(
                    this, statementContext.if_statement(), baseBody, allParams, result, structs
            );
            resultFlag = true;
        } else if (statementContext.loop_statement() != null) {
            LogicalOperation operation = visitorManager.getLoopStatementVisitor().loopVisitor(
                    this, statementContext.loop_statement(), baseBody, allParams, structs);
            if (operation != null) {
                result.add(operation);
            }
            resultFlag = true;
        }
        return resultFlag;
    }

    public LogicalOperation sqlStatementVisitor(
            PlSqlParser.Sql_statementContext sqlStatementContext,
            Map<String, PlType> declaredParams,
            Map<String, PlType> allParams,
            List<String> structs) {
        String originalSql = PLParserUtil.getFullText(sqlStatementContext);
        String sql;
        try {
            sql = PLParserUtil.getFullSQLWithParams(sqlStatementContext, allParams, structs);
        } catch (Exception e) {
            throw new ParseException("Failed to parse SQL statement: " + originalSql, e);
        }
        if (sql.length() < SQL_PREFIX_LEN) {
            return null;
        }
        String sqlType = sql.substring(0, SQL_PREFIX_LEN);
        switch (sqlType.toUpperCase()) {
            case "SELECT":
                return visitorManager.getSelectSQLVisitor()
                        .visitSelectSQL(originalSql, sql, declaredParams);
            case "INSERT":
                return visitorManager.getInsertSQLVisitor().visitInsertSQL(originalSql, sql);
            case "UPDATE":
                return visitorManager.getUpdateSQLVisitor().visitUpdateSQL(originalSql, sql);
            case "DELETE":
                return visitorManager.getDeleteSQLVisitor().visitDeleteSQL(originalSql, sql);
            case "MERGE ":
                return visitorManager.getMergeSQLVisitor().visitMergeSQL(originalSql, sql);
            case "CREATE":
                if (sqlStatementContext.data_manipulation_language_statements() != null &&
                        sqlStatementContext.data_manipulation_language_statements().create_table_as2() != null) {
                    String innerTableName = sqlStatementContext.data_manipulation_language_statements()
                            .create_table_as2().table_name().getText();
                    return visitorManager.getCreateAsSQLVisitor().visitCreateTableAsSQL(originalSql, sql, innerTableName);
                }
                throw new UnsupportedOperationException("Unsupported SQL syntax: " + sql);
            case "TRUNCA":
                if ("TRUNCATE".equalsIgnoreCase(sql.substring(0, 8))) {
                    return visitorManager.getTruncateSQLVisitor().visitTruncateSQL(sql);
                }
                throw new UnsupportedOperationException("Unsupported SQL syntax: " + sql);
            default:
                throw new UnsupportedOperationException("Unsupported SQL syntax: " + sql);
        }
    }
}
