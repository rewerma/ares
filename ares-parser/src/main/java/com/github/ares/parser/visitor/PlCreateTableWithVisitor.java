package com.github.ares.parser.visitor;

import static com.github.ares.api.common.CommonOptions.CONNECTOR;
import static com.github.ares.api.common.CommonOptions.DATA_SOURCE;
import static com.github.ares.parser.utils.Constants.DEFAULT_DATASOURCE_PATCHER;

import com.github.ares.com.fasterxml.jackson.core.JsonProcessingException;
import com.github.ares.com.fasterxml.jackson.core.type.TypeReference;
import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.org.antlr.v4.runtime.CharStream;
import com.github.ares.org.antlr.v4.runtime.CharStreams;
import com.github.ares.org.antlr.v4.runtime.CommonTokenStream;
import com.github.ares.parser.antlr4.CaseChangingCharStream;
import com.github.ares.parser.antlr4.CustomErrorListener;
import com.github.ares.parser.antlr4.sparksql.SqlBaseLexer;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import com.github.ares.parser.datasource.SourceConfigPatcher;
import com.github.ares.parser.datasource.SourceConfigPatcherFactory;
import com.github.ares.parser.hive.HiveTables;
import com.github.ares.parser.paimon.PaimonDdl;
import com.github.ares.parser.paimon.PaimonTables;
import com.github.ares.parser.plan.LogicalCreateHiveTable;
import com.github.ares.parser.plan.LogicalCreatePaimonTable;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalCreateTableAsSQL;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalSetConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

public class PlCreateTableWithVisitor {
    private static final String SOURCE_TYPE = "source";
    private static final String SINK_TYPE = "sink";

    @Inject private PlCreateSourceTableVisitor plCreateSourceTableVisitor;
    @Inject private PlCreateSinkTableVisitor plCreateSinkTableVisitor;
    @Inject private PlCreateAsSQLVisitor plCreateAsSQLVisitor;

    private Map<String, LogicalCreateSinkTable> sinkTables;
    private List<LogicalSetConfig> setConfigs = new ArrayList<>();

    public void init(PlVisitorManager plVisitorManager) {
        this.sinkTables = plVisitorManager.getSourceSinkTable().getSinkTables();
        plCreateSourceTableVisitor.init(plVisitorManager.getSourceSinkTable().getSourceTables());
    }

    public void setSetConfigs(List<LogicalSetConfig> setConfigs) {
        this.setConfigs = setConfigs == null ? new ArrayList<>() : setConfigs;
    }

    public List<LogicalOperation> visitCreate(String originalSql, String sql) {
        String rewritten = PaimonDdl.rewritePrimaryKey(normalizeSql(sql));
        SqlBaseParser.StatementContext statement = parseNormalized(rewritten).statement();
        if (statement instanceof SqlBaseParser.CreateViewContext) {
            SqlBaseParser.CreateViewContext view = (SqlBaseParser.CreateViewContext) statement;
            if (view.query() == null) {
                throw new ParseException("Unsupported SQL syntax: " + originalSql);
            }
            return single(plCreateAs(originalSql, sql, view.multipartIdentifier().getText()));
        }
        if (statement instanceof SqlBaseParser.CreateTableContext) {
            SqlBaseParser.CreateTableContext create = (SqlBaseParser.CreateTableContext) statement;
            String tableName = create.createTableHeader().multipartIdentifier().getText();
            if (isHiveNativeCreate(create)) {
                return visitHiveNativeTable(originalSql, sql, tableName);
            }
            if (isPaimonCreate(create)) {
                return visitPaimonCreate(
                        originalSql,
                        rewritten,
                        create,
                        tableName,
                        readOptions(create.createTableClauses()));
            }
            if (create.tableProvider() == null) {
                if (create.query() == null) {
                    throw new ParseException(
                            "CREATE TABLE requires USING connector OPTIONS (...), or a Hive CREATE TABLE definition: "
                                    + originalSql);
                }
                return single(plCreateAs(originalSql, sql, tableName));
            }
            String connector = create.tableProvider().multipartIdentifier().getText();
            Map<String, String> rawOptions = readOptions(create.createTableClauses());
            return visitConnectorTable(originalSql, tableName, connector, rawOptions);
        }
        if (statement instanceof SqlBaseParser.CreateTableLikeContext) {
            SqlBaseParser.CreateTableLikeContext like =
                    (SqlBaseParser.CreateTableLikeContext) statement;
            if (like.tableProvider() != null) {
                for (SqlBaseParser.TableProviderContext provider : like.tableProvider()) {
                    if (!HiveTables.isHiveConnector(provider.multipartIdentifier().getText())) {
                        throw new ParseException("Unsupported SQL syntax: " + originalSql);
                    }
                }
            }
            return visitHiveNativeTable(originalSql, sql, like.target.getText());
        }
        throw new ParseException("Unsupported SQL syntax: " + originalSql);
    }

    /**
     * Hive DDL defines the table itself. {@code USING hive OPTIONS ('table_name'=...)} still
     * registers a view over a table that already exists.
     */
    private static boolean isHiveNativeCreate(SqlBaseParser.CreateTableContext create) {
        String connector =
                create.tableProvider() == null
                        ? null
                        : create.tableProvider().multipartIdentifier().getText();
        if (connector != null && !HiveTables.isHiveConnector(connector)) {
            return false;
        }
        if (isAresRegistration(readOptions(create.createTableClauses()))) {
            return false;
        }
        boolean storage = hasHiveStorageClause(create.createTableClauses());
        boolean external = create.createTableHeader().EXTERNAL() != null;
        if (create.query() != null && !storage && !external) {
            return false;
        }
        if (connector != null) {
            return create.colTypeList() != null || storage || external || create.query() != null;
        }
        return create.colTypeList() != null || storage || external;
    }

    private static boolean isPaimonCreate(SqlBaseParser.CreateTableContext create) {
        if (create.tableProvider() == null
                || !PaimonTables.isPaimonConnector(
                        create.tableProvider().multipartIdentifier().getText())) {
            return false;
        }
        return create.colTypeList() != null || create.query() != null;
    }

    private static boolean isAresRegistration(Map<String, String> options) {
        for (String key : options.keySet()) {
            if ("table_name".equalsIgnoreCase(key)
                    || "type".equalsIgnoreCase(key)
                    || "datasource".equalsIgnoreCase(key)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasHiveStorageClause(SqlBaseParser.CreateTableClausesContext clauses) {
        if (clauses == null) {
            return false;
        }
        return !clauses.rowFormat().isEmpty()
                || !clauses.createFileFormat().isEmpty()
                || !clauses.locationSpec().isEmpty()
                || !clauses.bucketSpec().isEmpty()
                || !clauses.skewSpec().isEmpty()
                || !clauses.commentSpec().isEmpty()
                || (clauses.TBLPROPERTIES() != null && !clauses.TBLPROPERTIES().isEmpty())
                || (clauses.PARTITIONED() != null && !clauses.PARTITIONED().isEmpty());
    }

    private List<LogicalOperation> visitHiveNativeTable(
            String originalSql, String sql, String tableName) {
        String hiveTable = HiveTables.writableTable(tableName);
        LogicalCreateHiveTable createHiveTable = new LogicalCreateHiveTable();
        createHiveTable.setOriginSQL(originalSql);
        createHiveTable.setSql(sql);
        createHiveTable.setTableName(hiveTable);

        Map<String, Object> sinkOptions = new LinkedHashMap<>();
        sinkOptions.put(CONNECTOR.key(), HiveTables.CONNECTOR);
        sinkOptions.put("table_name", hiveTable);
        String normalized = hiveTable.toLowerCase(Locale.ROOT);
        LogicalCreateSinkTable sinkTable =
                plCreateSinkTableVisitor.visitCreateSinkTable(normalized, sinkOptions);
        if (sinkTables.containsKey(sinkTable.getTableName())) {
            throw new ParseException(
                    String.format("Sink table name exists: %s", sinkTable.getTableName()));
        }
        sinkTables.put(sinkTable.getTableName(), sinkTable);

        List<LogicalOperation> result = new ArrayList<>();
        result.add(createHiveTable);
        result.add(sinkTable);
        return result;
    }

    private LogicalOperation plCreateAs(String originalSql, String sql, String tableName) {
        return plCreateAsSQLVisitor.visitCreateInnerTable(originalSql, sql, tableName);
    }

    private List<LogicalOperation> visitConnectorTable(
            String originalSql,
            String tableName,
            String usingConnector,
            Map<String, String> rawOptions) {
        Map<String, String> merged = mergeConnectorOptions(usingConnector, rawOptions);
        String tableType = merged.remove("type");
        String standardType =
                tableType == null ? SOURCE_TYPE + "," + SINK_TYPE : normalizeType(tableType);

        Map<String, Object> options = parseOptionValues(merged);
        if (HiveTables.isHiveConnector(usingConnector)) {
            return visitHiveTable(originalSql, tableName, standardType, options);
        }
        if (PaimonTables.isPaimonConnector(usingConnector)) {
            return visitPaimonTable(originalSql, tableName, standardType, options);
        }
        List<LogicalOperation> result = new ArrayList<>();
        if (standardType.contains(SOURCE_TYPE)) {
            result.add(plCreateSourceTableVisitor.visitCreateSourceTable(tableName, options));
        }
        if (standardType.contains(SINK_TYPE)) {
            LogicalCreateSinkTable sinkTable =
                    plCreateSinkTableVisitor.visitCreateSinkTable(tableName, options);
            String sinkTableName = sinkTable.getTableName();
            if (sinkTables.containsKey(sinkTableName)) {
                throw new ParseException(
                        String.format("Sink table name exists: %s", sinkTableName));
            }
            sinkTables.put(sinkTableName, sinkTable);
            result.add(sinkTable);
        }
        return result;
    }

    /**
     * Spark clusters already integrate Hive, so {@code USING hive} becomes a temp view over the
     * Hive table. Writes go through Spark SQL against that table.
     */
    private List<LogicalOperation> visitHiveTable(
            String originalSql,
            String tableName,
            String standardType,
            Map<String, Object> options) {
        String hiveTable = HiveTables.requireTableName(options);
        String quotedHiveTable = HiveTables.quoteTable(hiveTable);
        String normalized = tableName.toLowerCase(Locale.ROOT);
        List<LogicalOperation> result = new ArrayList<>();
        if (standardType.contains(SOURCE_TYPE)) {
            String createSql = "CREATE VIEW " + normalized + " AS SELECT * FROM " + quotedHiveTable;
            result.add(plCreateAs(originalSql, createSql, normalized));
        }
        if (standardType.contains(SINK_TYPE)) {
            Map<String, Object> sinkOptions = new LinkedHashMap<>();
            sinkOptions.put(CONNECTOR.key(), HiveTables.CONNECTOR);
            sinkOptions.put("table_name", hiveTable);
            LogicalCreateSinkTable sinkTable =
                    plCreateSinkTableVisitor.visitCreateSinkTable(normalized, sinkOptions);
            if (sinkTables.containsKey(sinkTable.getTableName())) {
                throw new ParseException(
                        String.format("Sink table name exists: %s", sinkTable.getTableName()));
            }
            sinkTables.put(sinkTable.getTableName(), sinkTable);
            result.add(sinkTable);
        }
        return result;
    }

    /**
     * Column definitions create the Paimon table, then register the same alias used by {@code USING
     * paimon OPTIONS}.
     */
    private List<LogicalOperation> visitPaimonCreate(
            String originalSql,
            String source,
            SqlBaseParser.CreateTableContext create,
            String tableName,
            Map<String, String> rawOptions) {
        Map<String, String> merged = mergeConnectorOptions(PaimonTables.CONNECTOR, rawOptions);
        String tableType = merged.remove("type");
        String standardType =
                tableType == null ? SOURCE_TYPE + "," + SINK_TYPE : normalizeType(tableType);
        Map<String, Object> options = parseOptionValues(merged);
        Map<String, String> tableProperties = PaimonDdl.tableProperties(options);
        Map<String, Object> paimonOptions = PaimonTables.connectorOptions(options);
        LogicalCreatePaimonTable createPaimonTable = new LogicalCreatePaimonTable();
        createPaimonTable.setOriginSQL(originalSql);
        createPaimonTable.setSql(
                PaimonDdl.sparkDdl(create, source, paimonOptions, tableProperties));
        createPaimonTable.setTableName(PaimonTables.qualifiedTable(paimonOptions));
        createPaimonTable.setProperties(paimonOptions);

        List<LogicalOperation> result = new ArrayList<>();
        result.add(createPaimonTable);
        result.addAll(visitPaimonTable(originalSql, tableName, standardType, options));
        return result;
    }

    /**
     * Hive metastore follows the Hive view path. Filesystem metastore queries {@code
     * catalog.database.table} and can point {@code warehouse} at HDFS.
     */
    private List<LogicalOperation> visitPaimonTable(
            String originalSql,
            String tableName,
            String standardType,
            Map<String, Object> options) {
        Map<String, Object> paimonOptions = PaimonTables.connectorOptions(options);
        String quotedTable = PaimonTables.quoteTable(PaimonTables.qualifiedTable(paimonOptions));
        String normalized = tableName.toLowerCase(Locale.ROOT);
        List<LogicalOperation> result = new ArrayList<>();
        if (standardType.contains(SOURCE_TYPE)) {
            String createSql = "CREATE VIEW " + normalized + " AS SELECT * FROM " + quotedTable;
            LogicalOperation created = plCreateAs(originalSql, createSql, normalized);
            if (created instanceof LogicalCreateTableAsSQL) {
                ((LogicalCreateTableAsSQL) created).setProperties(paimonOptions);
            }
            result.add(created);
        }
        if (standardType.contains(SINK_TYPE)) {
            LogicalCreateSinkTable sinkTable =
                    plCreateSinkTableVisitor.visitCreateSinkTable(normalized, paimonOptions);
            if (sinkTables.containsKey(sinkTable.getTableName())) {
                throw new ParseException(
                        String.format("Sink table name exists: %s", sinkTable.getTableName()));
            }
            sinkTables.put(sinkTable.getTableName(), sinkTable);
            result.add(sinkTable);
        }
        return result;
    }

    private Map<String, String> mergeConnectorOptions(
            String usingConnector, Map<String, String> rawOptions) {
        Map<String, String> merged = new LinkedHashMap<>();
        String datasource = rawOptions.get(DATA_SOURCE.key());
        if (!StringUtils.isEmpty(datasource)) {
            Properties properties = new Properties();
            for (LogicalSetConfig setConfig : setConfigs) {
                properties.put(setConfig.getKey(), setConfig.getValue());
            }
            SourceConfigPatcher patcher =
                    SourceConfigPatcherFactory.getSourceConfigPatcher(DEFAULT_DATASOURCE_PATCHER);
            Map<String, String> patched = patcher.patchSourceConf(datasource.trim(), properties);
            if (patched != null) {
                merged.putAll(patched);
            }
        }
        rawOptions.forEach(
                (key, value) -> {
                    if (!DATA_SOURCE.key().equals(key)) {
                        merged.put(key, value);
                    }
                });
        if (!StringUtils.isEmpty(usingConnector)) {
            merged.put(CONNECTOR.key(), usingConnector);
        }
        return merged;
    }

    private static String normalizeType(String tableType) {
        String[] types = tableType.split(",");
        if (types.length == 1 && SOURCE_TYPE.equalsIgnoreCase(types[0].trim())) {
            return SOURCE_TYPE;
        }
        if (types.length == 1 && SINK_TYPE.equalsIgnoreCase(types[0].trim())) {
            return SINK_TYPE;
        }
        if (types.length == 2
                && ((SOURCE_TYPE.equalsIgnoreCase(types[0].trim())
                                && SINK_TYPE.equalsIgnoreCase(types[1].trim()))
                        || (SOURCE_TYPE.equalsIgnoreCase(types[1].trim())
                                && SINK_TYPE.equalsIgnoreCase(types[0].trim())))) {
            return SOURCE_TYPE + "," + SINK_TYPE;
        }
        throw new IllegalArgumentException(
                "The type of create table option must be 'source' or 'sink'");
    }

    private static Map<String, String> readOptions(
            SqlBaseParser.CreateTableClausesContext clauses) {
        Map<String, String> options = new LinkedHashMap<>();
        if (clauses == null || clauses.options == null) {
            return options;
        }
        for (SqlBaseParser.PropertyContext property : clauses.options.property()) {
            String key = unquote(property.propertyKey().getText());
            String value =
                    property.propertyValue() == null
                            ? ""
                            : unquote(property.propertyValue().getText());
            options.put(key, value);
        }
        return options;
    }

    private static Map<String, Object> parseOptionValues(Map<String, String> withOptions) {
        Map<String, Object> resultOptions = new LinkedHashMap<>();
        withOptions.forEach(
                (key, value) -> {
                    try {
                        if (value != null && value.trim().startsWith("[") && value.endsWith("]")) {
                            resultOptions.put(
                                    key,
                                    JsonUtils.OBJECT_MAPPER.readValue(
                                            value, new TypeReference<ArrayList<Object>>() {}));
                        } else if (value != null
                                && value.trim().startsWith("{")
                                && value.endsWith("}")) {
                            resultOptions.put(
                                    key,
                                    JsonUtils.OBJECT_MAPPER.readValue(
                                            value,
                                            new TypeReference<LinkedHashMap<String, Object>>() {}));
                        } else {
                            resultOptions.put(key, value);
                        }
                    } catch (JsonProcessingException e) {
                        throw new AresException(
                                "Create table with option json parse error: " + value);
                    }
                });
        return resultOptions;
    }

    private static String normalizeSql(String sql) {
        return sql.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ');
    }

    private static SqlBaseParser.SingleStatementContext parseSpark(String sql) {
        return parseNormalized(normalizeSql(sql));
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

    private static String unquote(String text) {
        if (text == null) {
            return null;
        }
        text = text.trim();
        if (text.length() >= 2) {
            char open = text.charAt(0);
            char close = text.charAt(text.length() - 1);
            if ((open == '\'' && close == '\'') || (open == '"' && close == '"')) {
                return text.substring(1, text.length() - 1);
            }
        }
        return text;
    }

    private static List<LogicalOperation> single(LogicalOperation operation) {
        List<LogicalOperation> result = new ArrayList<>();
        result.add(operation);
        return result;
    }
}
