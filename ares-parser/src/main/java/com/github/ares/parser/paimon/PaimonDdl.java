package com.github.ares.parser.paimon;

import com.github.ares.org.antlr.v4.runtime.ParserRuleContext;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Turns an Ares Paimon CREATE TABLE into Spark SQL that creates the catalog table. */
public final class PaimonDdl {
    private static final Pattern USING_PAIMON = Pattern.compile("(?i)\\bUSING\\s+paimon\\b");
    private static final Pattern PRIMARY_KEY =
            Pattern.compile(
                    "(?i)(\\s*,\\s*)?\\bPRIMARY\\s+KEY\\s*\\(([^)]*)\\)\\s*(?:NOT\\s+ENFORCED)?");
    private static final Pattern TBLPROPERTIES = Pattern.compile("(?i)\\bTBLPROPERTIES\\s*\\(");
    private static final Pattern OPTIONS = Pattern.compile("(?i)\\bOPTIONS\\s*\\(");
    private static final Pattern PRIMARY_KEY_PROPERTY =
            Pattern.compile("(?i)['\"]primary-key['\"]");

    private PaimonDdl() {}

    /**
     * The Spark parser has no PRIMARY KEY clause. Move {@code PRIMARY KEY (...) NOT ENFORCED} into
     * {@code TBLPROPERTIES ('primary-key'=...)} before parsing.
     */
    public static String rewritePrimaryKey(String sql) {
        if (sql == null || !USING_PAIMON.matcher(sql).find()) {
            return sql;
        }
        Matcher matcher = PRIMARY_KEY.matcher(sql);
        if (!matcher.find() || inQuote(sql, matcher.start())) {
            return sql;
        }
        String key = normalizeKeyColumns(matcher.group(2));
        String rewritten = sql.substring(0, matcher.start()) + sql.substring(matcher.end());
        rewritten = rewritten.replaceAll("\\(\\s*,", "(").replaceAll(",\\s*\\)", ")");
        if (key.isEmpty() || PRIMARY_KEY_PROPERTY.matcher(rewritten).find()) {
            return rewritten;
        }
        String property = "'primary-key'='" + key + "'";
        Matcher properties = TBLPROPERTIES.matcher(rewritten);
        if (properties.find()) {
            int insertAt = properties.end();
            return rewritten.substring(0, insertAt)
                    + property
                    + ", "
                    + rewritten.substring(insertAt);
        }
        String clause = " TBLPROPERTIES (" + property + ") ";
        Matcher options = OPTIONS.matcher(rewritten);
        if (options.find()) {
            return rewritten.substring(0, options.start())
                    + clause
                    + rewritten.substring(options.start());
        }
        return rewritten + clause;
    }

    public static String sparkDdl(
            SqlBaseParser.CreateTableContext create,
            String source,
            Map<String, Object> paimonOptions,
            Map<String, String> extraProperties) {
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        if (create.createTableHeader().EXISTS() != null) {
            sql.append("IF NOT EXISTS ");
        }
        sql.append(PaimonTables.quoteTable(PaimonTables.qualifiedTable(paimonOptions)));
        if (create.colTypeList() != null) {
            sql.append(" (");
            boolean first = true;
            for (SqlBaseParser.ColTypeContext column : create.colTypeList().colType()) {
                if (!first) {
                    sql.append(", ");
                }
                first = false;
                sql.append(slice(source, column).trim());
            }
            sql.append(')');
        }
        sql.append(" USING paimon");
        SqlBaseParser.CreateTableClausesContext clauses = create.createTableClauses();
        if (clauses != null && clauses.partitioning != null) {
            sql.append(" PARTITIONED BY (");
            boolean first = true;
            for (SqlBaseParser.PartitionFieldContext field :
                    clauses.partitioning.partitionField()) {
                if (!first) {
                    sql.append(", ");
                }
                first = false;
                sql.append(partitionItem(field));
            }
            sql.append(')');
        }
        if (clauses != null && !clauses.commentSpec().isEmpty()) {
            sql.append(' ').append(slice(source, clauses.commentSpec(0)).trim());
        }
        if (clauses != null && !clauses.bucketSpec().isEmpty()) {
            sql.append(' ').append(slice(source, clauses.bucketSpec(0)).trim());
        }
        Map<String, String> properties = tableProperties(clauses, extraProperties);
        if (!properties.isEmpty()) {
            sql.append(" TBLPROPERTIES (");
            boolean first = true;
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (!first) {
                    sql.append(", ");
                }
                first = false;
                sql.append('\'')
                        .append(escapeQuote(entry.getKey()))
                        .append("'='")
                        .append(escapeQuote(entry.getValue()))
                        .append('\'');
            }
            sql.append(')');
        }
        if (create.query() != null) {
            sql.append(" AS ").append(slice(source, create.query()).trim());
        }
        return sql.toString();
    }

    /** OPTIONS entries that belong on the Paimon table, such as {@code bucket}. */
    public static Map<String, String> tableProperties(Map<String, Object> options) {
        Map<String, String> properties = new LinkedHashMap<>();
        if (options == null) {
            return properties;
        }
        for (Map.Entry<String, Object> entry : options.entrySet()) {
            if (entry.getValue() == null || isCatalogOption(entry.getKey())) {
                continue;
            }
            properties.put(entry.getKey(), entry.getValue().toString());
        }
        return properties;
    }

    public static String slice(String source, ParserRuleContext context) {
        return source.substring(
                context.getStart().getStartIndex(), context.getStop().getStopIndex() + 1);
    }

    private static Map<String, String> tableProperties(
            SqlBaseParser.CreateTableClausesContext clauses, Map<String, String> extraProperties) {
        Map<String, String> properties = new LinkedHashMap<>();
        if (clauses != null && clauses.tableProps != null) {
            for (SqlBaseParser.PropertyContext property : clauses.tableProps.property()) {
                String key = unquote(property.propertyKey().getText());
                String value =
                        property.propertyValue() == null
                                ? ""
                                : unquote(property.propertyValue().getText());
                properties.put(key, value);
            }
        }
        if (extraProperties != null) {
            for (Map.Entry<String, String> entry : extraProperties.entrySet()) {
                if (!properties.containsKey(entry.getKey())) {
                    properties.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return properties;
    }

    private static String partitionItem(SqlBaseParser.PartitionFieldContext field) {
        if (field instanceof SqlBaseParser.PartitionColumnContext) {
            return quoteIdent(
                    ((SqlBaseParser.PartitionColumnContext) field).colType().colName.getText());
        }
        String text = field.getText();
        if (text.matches("[A-Za-z_][A-Za-z0-9_$]*")) {
            return quoteIdent(text);
        }
        return text;
    }

    private static boolean isCatalogOption(String key) {
        String normalized = key.toLowerCase(Locale.ROOT);
        return "connector".equals(normalized)
                || "metastore".equals(normalized)
                || "warehouse".equals(normalized)
                || "table_name".equals(normalized)
                || "type".equals(normalized)
                || "datasource".equals(normalized)
                || "catalog".equals(normalized)
                || "fs.defaultfs".equals(normalized)
                || "hdfs_site_path".equals(normalized)
                || "url".equals(normalized)
                || "driver".equals(normalized)
                || "user".equals(normalized)
                || "username".equals(normalized)
                || "password".equals(normalized)
                || normalized.startsWith("fs.")
                || normalized.startsWith("dfs.")
                || normalized.startsWith("hadoop.");
    }

    private static String normalizeKeyColumns(String columns) {
        String[] parts = columns.split(",");
        StringBuilder key = new StringBuilder();
        for (String part : parts) {
            String column = unquote(part.trim());
            if (column.isEmpty()) {
                continue;
            }
            if (key.length() > 0) {
                key.append(',');
            }
            key.append(column);
        }
        return key.toString();
    }

    private static String quoteIdent(String identifier) {
        return "`" + unquote(identifier).replace("`", "``") + "`";
    }

    private static String unquote(String text) {
        if (text == null) {
            return "";
        }
        text = text.trim();
        if (text.length() >= 2) {
            char open = text.charAt(0);
            char close = text.charAt(text.length() - 1);
            if ((open == '`' && close == '`')
                    || (open == '\'' && close == '\'')
                    || (open == '"' && close == '"')) {
                return text.substring(1, text.length() - 1);
            }
        }
        return text;
    }

    private static String escapeQuote(String text) {
        return text.replace("'", "''");
    }

    private static boolean inQuote(String sql, int index) {
        boolean quoted = false;
        for (int i = 0; i < index && i < sql.length(); i++) {
            if (sql.charAt(i) != '\'') {
                continue;
            }
            if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                i++;
                continue;
            }
            quoted = !quoted;
        }
        return quoted;
    }
}
