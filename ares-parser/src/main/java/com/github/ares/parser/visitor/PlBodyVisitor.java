package com.github.ares.parser.visitor;

import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalContinueLoop;
import com.github.ares.parser.plan.LogicalExitLoop;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.utils.PLParserUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PlBodyVisitor {
    private static final int SQL_PREFIX_LEN = 6;

    protected PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public List<LogicalOperation> visitBodyStatements(List<PlSqlParser.StatementContext> statementContextList, Map<String, PlType> inParams,
                                                      Map<String, PlType> outParams, Map<String, PlType> declaredParams,
                                                      List<LogicalOperation> baseBody, List<String> structs) {
        Map<String, PlType> allParams = new LinkedHashMap<>(inParams);
        declaredParams.putAll(outParams);
        allParams.putAll(declaredParams);
        if (statementContextList == null) {
            return null;
        }
        List<LogicalOperation> result = new ArrayList<>();
        for (PlSqlParser.StatementContext statementContext : statementContextList) {
            PlSqlParser.Exit_statementContext exitStatementContext = statementContext.exit_statement();
            if (exitStatementContext != null) {
                result.add(new LogicalExitLoop());
                continue;
            }
            PlSqlParser.Continue_statementContext continueStatementContext = statementContext.continue_statement();
            if (continueStatementContext != null) {
                result.add(new LogicalContinueLoop());
                continue;
            }

            PlSqlParser.Call_statementContext callStatementContext = statementContext.call_statement();
            if (callStatementContext != null) {
                LogicalOperation operation = visitorManager.getCallStatementVisitor()
                        .visitCallStatement(callStatementContext, allParams, baseBody, result, structs);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            PlSqlParser.Sql_statementContext sqlStatementContext = statementContext.sql_statement();
            if (sqlStatementContext != null) {
                LogicalOperation operation = sqlStatementVisitor(sqlStatementContext, declaredParams, allParams, structs);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            PlSqlParser.Assignment_statementContext assignmentStatement = statementContext.assignment_statement();
            if (assignmentStatement != null) {
                LogicalOperation operation = visitorManager.getAssignmentVisitor()
                        .visitAssignment(assignmentStatement, declaredParams, allParams, structs);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }
            PlSqlParser.If_statementContext ifStatement = statementContext.if_statement();
            if (ifStatement != null) {
                visitorManager.getIfStatementVisitor().ifElseVisitor(
                        this, ifStatement, baseBody, allParams, result, structs
                );
                continue;
            }

            PlSqlParser.Loop_statementContext loopStatementContext = statementContext.loop_statement();
            if (loopStatementContext != null) {
                LogicalOperation operation = visitorManager.getLoopStatementVisitor().loopVisitor(
                        this, loopStatementContext, baseBody, allParams, structs);
                if (operation != null) {
                    result.add(operation);
                }
                continue;
            }

            PlSqlParser.Raise_statementContext raiseStatementContext = statementContext.raise_statement();
            if (raiseStatementContext != null) {
                continue;
            }

            throw new UnsupportedOperationException(String.format("Unsupported syntax: '%s' in body",
                    PLParserUtil.getFullText(statementContext)));
        }
        return result;
    }

    public List<LogicalOperation> visitBodyStatements(PlSqlParser.Seq_of_statementsContext seq_of_statementsContext, Map<String, PlType> inParams,
                                                      Map<String, PlType> outParams, Map<String, PlType> declaredParams,
                                                      List<LogicalOperation> baseBody, List<String> structs) {
        return visitBodyStatements(seq_of_statementsContext.statement(), inParams, outParams, declaredParams, baseBody, structs);
    }

    public LogicalOperation sqlStatementVisitor(PlSqlParser.Sql_statementContext sql_statementContext,
                                                Map<String, PlType> declaredParams, Map<String, PlType> allParams, List<String> structs) {
        String originalSql = PLParserUtil.getFullText(sql_statementContext);
        String sql;
        try {
            sql = PLParserUtil.getFullSQLWithParams(sql_statementContext, allParams, structs);
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
                if (sql_statementContext.data_manipulation_language_statements() != null &&
                        sql_statementContext.data_manipulation_language_statements().create_table_as2() != null) {
                    String innerTableName = sql_statementContext.data_manipulation_language_statements()
                            .create_table_as2().table_name().getText();
                    return visitorManager.getCreateAsSQLVisitor().visitCreateInnerTable(originalSql, sql, innerTableName);
                }
            case "TRUNCA":
                if ("TRUNCATE".equalsIgnoreCase(sql.substring(0, 8))) {
                    return visitorManager.getTruncateSQLVisitor().visitTruncateSQL(sql);
                }
            default:
                throw new UnsupportedOperationException("Unsupported SQL syntax: " + sql);
        }

    }
}
