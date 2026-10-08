package com.github.ares.parser.visitor;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.hive.HiveTables;
import com.github.ares.parser.paimon.PaimonTables;
import com.github.ares.parser.plan.LogicalAssignment;
import com.github.ares.parser.plan.LogicalCommit;
import com.github.ares.parser.plan.LogicalContinueLoop;
import com.github.ares.parser.plan.LogicalCreateHiveTable;
import com.github.ares.parser.plan.LogicalCreatePaimonTable;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalCreateTableAsSQL;
import com.github.ares.parser.plan.LogicalEndTransaction;
import com.github.ares.parser.plan.LogicalExitLoop;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalRollback;
import com.github.ares.parser.plan.LogicalSetConfig;
import com.github.ares.parser.plan.LogicalStartTransaction;
import com.github.ares.parser.utils.PLParserUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PlBodyVisitor {
    protected PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public List<LogicalOperation> visitBlock(
            PlSqlParser.BodyContext body,
            Map<String, PlType> scriptDeclared,
            Map<String, PlType> visible,
            List<String> structs) {
        if (body == null || body.statement() == null) {
            return Collections.emptyList();
        }
        List<LogicalOperation> result = new ArrayList<>();
        for (PlSqlParser.StatementContext statement : body.statement()) {
            result.addAll(visitStatement(statement, scriptDeclared, visible, structs, false));
        }
        return result;
    }

    public List<LogicalOperation> visitStatement(
            PlSqlParser.StatementContext statement,
            Map<String, PlType> scriptDeclared,
            Map<String, PlType> visible,
            List<String> structs,
            boolean topLevel) {
        if (statement.if_statement() != null) {
            List<LogicalOperation> result = new ArrayList<>();
            visitorManager
                    .getIfStatementVisitor()
                    .ifElseVisitor(
                            this,
                            statement.if_statement(),
                            scriptDeclared,
                            visible,
                            result,
                            structs);
            return result;
        }
        if (statement.while_statement() != null) {
            return Collections.singletonList(
                    visitorManager
                            .getLoopStatementVisitor()
                            .visitWhile(
                                    this,
                                    statement.while_statement(),
                                    scriptDeclared,
                                    visible,
                                    structs));
        }
        if (statement.for_statement() != null) {
            return Collections.singletonList(
                    visitorManager
                            .getLoopStatementVisitor()
                            .visitFor(
                                    this,
                                    statement.for_statement(),
                                    scriptDeclared,
                                    visible,
                                    structs));
        }
        if (statement.try_statement() != null) {
            return Collections.singletonList(
                    visitorManager
                            .getExceptionHandlerVisitor()
                            .visitTry(
                                    this,
                                    statement.try_statement(),
                                    scriptDeclared,
                                    visible,
                                    structs));
        }
        PlSqlParser.Terminated_statementContext terminated = statement.terminated_statement();
        if (terminated == null) {
            throw unsupported(statement);
        }
        if (terminated.def_statement() != null) {
            return visitDef(terminated.def_statement(), scriptDeclared, visible, structs);
        }
        if (terminated.assignment_statement() != null) {
            return Collections.singletonList(
                    visitorManager
                            .getAssignmentVisitor()
                            .visitAssignment(terminated.assignment_statement(), visible, structs));
        }
        if (terminated.call_statement() != null) {
            return Collections.singletonList(
                    visitorManager
                            .getCallStatementVisitor()
                            .visitCallStatement(terminated.call_statement(), visible, structs));
        }
        if (terminated.break_statement() != null) {
            return Collections.singletonList(new LogicalExitLoop());
        }
        if (terminated.continue_statement() != null) {
            return Collections.singletonList(new LogicalContinueLoop());
        }
        if (terminated.raise_statement() != null) {
            throw new ParseException("raise is only valid in a catch block");
        }
        if (terminated.transaction_statement() != null) {
            return Collections.singletonList(
                    toTransactionOperation(terminated.transaction_statement()));
        }
        if (terminated.set_statement() != null) {
            if (!topLevel) {
                throw new ParseException("SET must be a top-level statement");
            }
            return Collections.singletonList(toSetConfig(terminated.set_statement()));
        }
        if (terminated.sql_statement() != null) {
            return visitSql(terminated.sql_statement(), scriptDeclared, visible, structs, topLevel);
        }
        throw unsupported(statement);
    }

    public static LogicalSetConfig toSetConfig(PlSqlParser.Set_statementContext setStatement) {
        String setBlock = PLParserUtil.getFullText(setStatement);
        if (setBlock.length() >= 3 && setBlock.regionMatches(true, 0, "SET", 0, 3)) {
            setBlock = setBlock.substring(3);
        }
        if (setBlock.endsWith(";")) {
            setBlock = setBlock.substring(0, setBlock.length() - 1);
        }
        int equalIndex = setBlock.indexOf("=");
        LogicalSetConfig setConfig = new LogicalSetConfig();
        if (equalIndex > -1) {
            setConfig.setKey(setBlock.substring(0, equalIndex).trim());
            setConfig.setValue(
                    PLParserUtil.stripOptionalSingleQuotes(
                            setBlock.substring(equalIndex + 1).trim()));
        } else {
            setConfig.setKey(setBlock.trim());
        }
        return setConfig;
    }

    private List<LogicalOperation> visitDef(
            PlSqlParser.Def_statementContext defStatement,
            Map<String, PlType> scriptDeclared,
            Map<String, PlType> visible,
            List<String> structs) {
        String name = identText(defStatement.identifier());
        if (scriptDeclared.containsKey(name) || visible.containsKey(name)) {
            throw new ParseException("Parameter is already defined: " + name);
        }
        PlType type =
                defStatement.expression() == null
                        ? PlType.of(InternalFieldType.VARCHAR)
                        : inferType(defStatement.expression(), visible);
        scriptDeclared.put(name, type);
        if (visible != scriptDeclared) {
            visible.put(name, type);
        }
        if (defStatement.expression() == null) {
            return Collections.emptyList();
        }
        LogicalAssignment assignment = new LogicalAssignment();
        com.github.ares.parser.model.Argument argument =
                new com.github.ares.parser.model.Argument(name, type);
        assignment.setParam(argument);
        assignment.setExpr(
                visitorManager
                        .getExpressionVisitor()
                        .visitExpressionContext(defStatement.expression(), visible, structs)
                        .getExpr());
        return Collections.singletonList(assignment);
    }

    private List<LogicalOperation> visitSql(
            PlSqlParser.Sql_statementContext sqlStatement,
            Map<String, PlType> scriptDeclared,
            Map<String, PlType> visible,
            List<String> structs,
            boolean topLevel) {
        String originalSql = PLParserUtil.getFullText(sqlStatement);
        String sql = PLParserUtil.getFullSQLWithParams(sqlStatement, visible, structs);
        sql = PLParserUtil.cleanSQL(sql);
        originalSql = PLParserUtil.cleanSQL(originalSql);
        PlSqlParser.Sql_prefixContext prefix = sqlStatement.sql_prefix();
        if (prefix.SELECT() != null || prefix.WITH() != null) {
            return Collections.singletonList(
                    visitorManager
                            .getSelectSQLVisitor()
                            .visitSelectSQL(originalSql, sql, scriptDeclared));
        }
        if (prefix.INSERT() != null) {
            return Collections.singletonList(
                    visitorManager.getInsertSQLVisitor().visitInsertSQL(originalSql, sql));
        }
        if (prefix.UPDATE() != null) {
            return Collections.singletonList(
                    visitorManager.getUpdateSQLVisitor().visitUpdateSQL(originalSql, sql));
        }
        if (prefix.DELETE() != null) {
            return Collections.singletonList(
                    visitorManager.getDeleteSQLVisitor().visitDeleteSQL(originalSql, sql));
        }
        if (prefix.MERGE() != null) {
            return Collections.singletonList(
                    visitorManager.getMergeSQLVisitor().visitMergeSQL(originalSql, sql));
        }
        if (prefix.TRUNCATE() != null) {
            return Collections.singletonList(
                    visitorManager.getTruncateSQLVisitor().visitTruncateSQL(sql));
        }
        if (prefix.CREATE() != null) {
            List<LogicalOperation> created =
                    visitorManager.getCreateTableWithVisitor().visitCreate(originalSql, sql);
            if (containsCatalogTable(created) && !topLevel) {
                throw new ParseException("CREATE TABLE ... USING must be a top-level statement");
            }
            return created;
        }
        throw new ParseException("Unsupported SQL syntax: " + originalSql);
    }

    public static boolean containsCatalogTable(List<LogicalOperation> operations) {
        for (LogicalOperation operation : operations) {
            if (operation instanceof LogicalCreateSourceTable
                    || operation instanceof LogicalCreateSinkTable
                    || operation instanceof LogicalCreateHiveTable
                    || operation instanceof LogicalCreatePaimonTable
                    || operation instanceof LogicalSetConfig) {
                return true;
            }
            if (operation instanceof LogicalCreateTableAsSQL
                    && (HiveTables.scriptUsesHive(
                                    ((LogicalCreateTableAsSQL) operation).getOriginSQL())
                            || PaimonTables.scriptUsesPaimon(
                                    ((LogicalCreateTableAsSQL) operation).getOriginSQL()))) {
                return true;
            }
        }
        return false;
    }

    static LogicalOperation toTransactionOperation(PlSqlParser.Transaction_statementContext tx) {
        if (tx.COMMIT() != null) {
            return new LogicalCommit();
        }
        if (tx.ROLLBACK() != null) {
            return new LogicalRollback();
        }
        if (tx.END() != null) {
            return new LogicalEndTransaction();
        }
        return new LogicalStartTransaction();
    }

    static String identText(PlSqlParser.IdentifierContext identifier) {
        String text = identifier.getText();
        if (text.length() >= 2) {
            char quote = text.charAt(0);
            if ((quote == '"' || quote == '`') && text.charAt(text.length() - 1) == quote) {
                return text.substring(1, text.length() - 1);
            }
        }
        return text;
    }

    private static PlType inferType(
            PlSqlParser.ExpressionContext expression, Map<String, PlType> visible) {
        String text = expression.getText();
        if (text == null) {
            return PlType.of(InternalFieldType.VARCHAR);
        }
        if ("true".equalsIgnoreCase(text) || "false".equalsIgnoreCase(text)) {
            return PlType.of(InternalFieldType.BOOLEAN);
        }
        if (text.length() >= 2
                && ((text.charAt(0) == '\'' && text.charAt(text.length() - 1) == '\'')
                        || (text.charAt(0) == 'N' || text.charAt(0) == 'n')
                                && text.charAt(1) == '\'')) {
            return PlType.of(InternalFieldType.VARCHAR);
        }
        String number = text.startsWith("+") ? text.substring(1) : text;
        if (number.startsWith("-")) {
            number = number.substring(1);
        }
        if (number.matches("\\d+")) {
            return number.length() > 9
                    ? PlType.of(InternalFieldType.LONG)
                    : PlType.of(InternalFieldType.INT);
        }
        if (number.matches("\\d+\\.\\d+([eE][+-]?\\d+)?[dDfF]?")) {
            return PlType.of(InternalFieldType.DOUBLE);
        }
        PlType known = visible.get(text);
        if (known != null) {
            return known;
        }
        return PlType.of(InternalFieldType.VARCHAR);
    }

    private static ParseException unsupported(PlSqlParser.StatementContext statement) {
        return new ParseException("Unsupported syntax: " + PLParserUtil.getFullText(statement));
    }
}
