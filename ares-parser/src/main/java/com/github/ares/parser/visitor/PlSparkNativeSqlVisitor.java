package com.github.ares.parser.visitor;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.org.antlr.v4.runtime.CharStream;
import com.github.ares.org.antlr.v4.runtime.CharStreams;
import com.github.ares.org.antlr.v4.runtime.CommonTokenStream;
import com.github.ares.org.antlr.v4.runtime.tree.ParseTree;
import com.github.ares.parser.antlr4.CaseChangingCharStream;
import com.github.ares.parser.antlr4.CustomErrorListener;
import com.github.ares.parser.antlr4.sparksql.SqlBaseLexer;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import com.github.ares.parser.hive.HiveTables;
import com.github.ares.parser.model.SourceSinkTable;
import com.github.ares.parser.model.TableWith;
import com.github.ares.parser.paimon.PaimonTables;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalSparkNativeSql;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PlSparkNativeSqlVisitor {
    private SourceSinkTable sourceSinkTable;
    private Map<String, LogicalCreateSourceTable> sourceTables;
    private Map<String, LogicalCreateSinkTable> sinkTables;

    public void init(PlVisitorManager plVisitorManager) {
        this.sourceSinkTable = plVisitorManager.getSourceSinkTable();
        this.sourceTables = sourceSinkTable.getSourceTables();
        this.sinkTables = sourceSinkTable.getSinkTables();
    }

    /**
     * Spark statements executed as written: cache management ({@code CACHE TABLE}, {@code UNCACHE
     * TABLE [IF EXISTS]}, {@code CLEAR CACHE}), temporary views, and {@code ALTER TABLE|VIEW ...
     * ADD [IF NOT EXISTS] PARTITION} / {@code DROP [IF EXISTS] PARTITION}. A registered Hive or
     * Paimon alias used as a table is replaced with its catalog {@code table_name}.
     */
    public List<LogicalOperation> visit(String originalSql, String sql) {
        String normalized = normalizeSql(sql);
        SqlBaseParser.StatementContext statement = parseNormalized(normalized).statement();
        if (statement instanceof SqlBaseParser.CacheTableContext
                || statement instanceof SqlBaseParser.UncacheTableContext
                || statement instanceof SqlBaseParser.ClearCacheContext
                || statement instanceof SqlBaseParser.AddTablePartitionContext
                || statement instanceof SqlBaseParser.DropTablePartitionsContext) {
            return single(
                    toNativeSql(originalSql, rewriteCatalogTables(sql, normalized, statement)));
        }
        throw new ParseException("Unsupported SQL syntax: " + originalSql);
    }

    public List<LogicalOperation> visitTempView(String originalSql, String sql, String viewName) {
        return visitTempView(originalSql, sql, viewName, false, false);
    }

    public List<LogicalOperation> visitTempView(
            String originalSql,
            String sql,
            String viewName,
            boolean orReplace,
            boolean ifNotExists) {
        registerTempViewSource(viewName, orReplace, ifNotExists);
        String normalized = normalizeSql(sql);
        SqlBaseParser.StatementContext statement = parseNormalized(normalized).statement();
        if (statement instanceof SqlBaseParser.CreateViewContext
                && ((SqlBaseParser.CreateViewContext) statement).query() != null) {
            return single(
                    toNativeSql(originalSql, rewriteCatalogTables(sql, normalized, statement)));
        }
        return single(toNativeSql(originalSql, sql));
    }

    private void registerTempViewSource(String viewName, boolean orReplace, boolean ifNotExists) {
        String normalized = normalizeViewName(viewName);
        if (sourceTables.containsKey(normalized)) {
            if (orReplace || ifNotExists) {
                return;
            }
            throw new ParseException(String.format("Source table: %s already exists", normalized));
        }
        sourceTables.put(normalized, null);
    }

    static String normalizeViewName(String viewName) {
        String normalized = SourceSinkTable.normalizeAlias(viewName);
        int dot = normalized.lastIndexOf('.');
        if (dot >= 0) {
            normalized = normalized.substring(dot + 1);
        }
        return normalized;
    }

    /**
     * Rewrites catalog aliases inside {@code normalized}. When nothing changes, the caller's
     * original {@code sql} is kept so whitespace stays as written.
     */
    private String rewriteCatalogTables(
            String sql, String normalized, SqlBaseParser.StatementContext statement) {
        List<SqlBaseParser.MultipartIdentifierContext> tables = new ArrayList<>();
        boolean rejectOtherConnector = false;
        if (statement instanceof SqlBaseParser.CacheTableContext) {
            SqlBaseParser.CacheTableContext cache = (SqlBaseParser.CacheTableContext) statement;
            if (cache.query() == null) {
                tables.add(cache.multipartIdentifier());
            } else {
                collectQueryTables(cache.query(), tables);
            }
        } else if (statement instanceof SqlBaseParser.UncacheTableContext) {
            tables.add(((SqlBaseParser.UncacheTableContext) statement).multipartIdentifier());
        } else if (statement instanceof SqlBaseParser.CreateViewContext) {
            collectQueryTables(((SqlBaseParser.CreateViewContext) statement).query(), tables);
        } else if (statement instanceof SqlBaseParser.AddTablePartitionContext
                || statement instanceof SqlBaseParser.DropTablePartitionsContext) {
            tables.add(partitionTable(statement));
            rejectOtherConnector = true;
        }
        String rewritten = replaceAliases(normalized, tables, rejectOtherConnector);
        return rewritten.equals(normalized) ? sql : rewritten;
    }

    private void collectQueryTables(
            ParseTree node, List<SqlBaseParser.MultipartIdentifierContext> tables) {
        Set<String> cteNames = new HashSet<>();
        collectCteNames(node, cteNames);
        collectTableNames(node, cteNames, tables);
    }

    private static void collectCteNames(ParseTree node, Set<String> cteNames) {
        if (node instanceof SqlBaseParser.NamedQueryContext) {
            cteNames.add(
                    SourceSinkTable.normalizeAlias(
                            ((SqlBaseParser.NamedQueryContext) node).name.getText()));
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectCteNames(node.getChild(i), cteNames);
        }
    }

    private static void collectTableNames(
            ParseTree node,
            Set<String> cteNames,
            List<SqlBaseParser.MultipartIdentifierContext> tables) {
        if (node instanceof SqlBaseParser.TableNameContext) {
            SqlBaseParser.MultipartIdentifierContext identifier =
                    ((SqlBaseParser.TableNameContext) node).multipartIdentifier();
            if (!cteNames.contains(SourceSinkTable.normalizeAlias(identifier.getText()))) {
                tables.add(identifier);
            }
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectTableNames(node.getChild(i), cteNames, tables);
        }
    }

    private String replaceAliases(
            String sql,
            List<SqlBaseParser.MultipartIdentifierContext> tables,
            boolean rejectOtherConnector) {
        List<SqlBaseParser.MultipartIdentifierContext> matched = new ArrayList<>();
        for (SqlBaseParser.MultipartIdentifierContext identifier : tables) {
            String alias = SourceSinkTable.normalizeAlias(identifier.getText());
            if (sourceSinkTable.cachedCatalogTable(alias) != null) {
                matched.add(identifier);
            } else if (rejectOtherConnector) {
                rejectUnsupportedConnector(alias);
            }
        }
        matched.sort(
                (left, right) ->
                        Integer.compare(
                                right.getStart().getStartIndex(), left.getStart().getStartIndex()));
        String rewritten = sql;
        for (SqlBaseParser.MultipartIdentifierContext identifier : matched) {
            String quoted =
                    sourceSinkTable.cachedCatalogTable(
                            SourceSinkTable.normalizeAlias(identifier.getText()));
            int start = identifier.getStart().getStartIndex();
            int stop = identifier.getStop().getStopIndex();
            rewritten = rewritten.substring(0, start) + quoted + rewritten.substring(stop + 1);
        }
        return rewritten;
    }

    private void rejectUnsupportedConnector(String alias) {
        TableWith registered = sinkTables.get(alias);
        if (registered == null) {
            registered = sourceTables.get(alias);
        }
        if (registered == null || registered.getConnector() == null) {
            return;
        }
        String connector = registered.getConnector();
        if (HiveTables.isHiveConnector(connector) || PaimonTables.isPaimonConnector(connector)) {
            return;
        }
        throw new ParseException(
                "ALTER TABLE PARTITION is not supported for connector "
                        + connector
                        + ": "
                        + registered.getTableName());
    }

    private static SqlBaseParser.MultipartIdentifierContext partitionTable(
            SqlBaseParser.StatementContext statement) {
        if (statement instanceof SqlBaseParser.AddTablePartitionContext) {
            return ((SqlBaseParser.AddTablePartitionContext) statement).multipartIdentifier();
        }
        return ((SqlBaseParser.DropTablePartitionsContext) statement).multipartIdentifier();
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
