package com.github.ares.parser.visitor;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.org.antlr.v4.runtime.CharStream;
import com.github.ares.org.antlr.v4.runtime.CharStreams;
import com.github.ares.org.antlr.v4.runtime.CommonTokenStream;
import com.github.ares.parser.antlr4.CaseChangingCharStream;
import com.github.ares.parser.antlr4.CustomErrorListener;
import com.github.ares.parser.antlr4.sparksql.SqlBaseLexer;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalSparkNativeSql;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class PlSparkNativeSqlVisitor {
    private Map<String, LogicalCreateSourceTable> sourceTables;

    public void init(PlVisitorManager plVisitorManager) {
        this.sourceTables = plVisitorManager.getSourceSinkTable().getSourceTables();
    }

    /** Spark cache management: {@code CACHE TABLE}, {@code UNCACHE TABLE [IF EXISTS]}, {@code CLEAR CACHE}. */
    public List<LogicalOperation> visit(String originalSql, String sql) {
        SqlBaseParser.StatementContext statement =
                parseNormalized(normalizeSql(sql)).statement();
        if (statement instanceof SqlBaseParser.CacheTableContext
                || statement instanceof SqlBaseParser.UncacheTableContext
                || statement instanceof SqlBaseParser.ClearCacheContext) {
            return single(toNativeSql(originalSql, sql));
        }
        throw new ParseException("Unsupported SQL syntax: " + originalSql);
    }

    public List<LogicalOperation> visitTempView(
            String originalSql, String sql, String viewName) {
        registerTempViewSource(viewName);
        return single(toNativeSql(originalSql, sql));
    }

    private void registerTempViewSource(String viewName) {
        String normalized = viewName.toLowerCase(Locale.ROOT);
        int dot = normalized.lastIndexOf('.');
        if (dot >= 0) {
            normalized = normalized.substring(dot + 1);
        }
        if (sourceTables.containsKey(normalized)) {
            throw new ParseException(
                    String.format("Source table: %s already exists", normalized));
        }
        sourceTables.put(normalized, null);
    }

    private static LogicalSparkNativeSql toNativeSql(String originalSql, String sql) {
        LogicalSparkNativeSql operation = new LogicalSparkNativeSql();
        operation.setOriginSQL(originalSql);
        operation.setSql(sql);
        return operation;
    }

    private static String normalizeSql(String sql) {
        return sql.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ');
    }

    private static SqlBaseParser.SingleStatementContext parseNormalized(String sql) {
        CharStream stream = CharStreams.fromString(sql);
        CaseChangingCharStream upper = new CaseChangingCharStream(stream, true);
        CustomErrorListener lexerErrors = new CustomErrorListener();
        SqlBaseLexer lexer = new SqlBaseLexer(upper);
        lexer.removeErrorListeners();
        lexer.addErrorListener(lexerErrors);
        SqlBaseParser parser = new SqlBaseParser(new CommonTokenStream(lexer));
        CustomErrorListener parserErrors = new CustomErrorListener();
        parser.removeErrorListeners();
        parser.addErrorListener(parserErrors);
        SqlBaseParser.SingleStatementContext statement = parser.singleStatement();
        if (!lexerErrors.getErrors().isEmpty() || !parserErrors.getErrors().isEmpty()) {
            List<String> errors = new ArrayList<>(lexerErrors.getErrors());
            errors.addAll(parserErrors.getErrors());
            throw new ParseException(String.join("\n", errors) + "\n" + sql);
        }
        return statement;
    }

    private static List<LogicalOperation> single(LogicalOperation operation) {
        List<LogicalOperation> result = new ArrayList<>();
        result.add(operation);
        return result;
    }
}
