package com.github.ares.parser.hive;

import com.github.ares.common.exceptions.ParseException;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Hive tables are Spark catalog tables, not a connector plugin. */
public final class HiveTables {
    public static final String CONNECTOR = "hive";

    private static final Pattern USING_HIVE = Pattern.compile("(?i)\\bUSING\\s+hive3?\\b");
    private static final Pattern CREATE_TABLE_STMT =
            Pattern.compile(
                    "(?is)\\bCREATE\\s+(?:TEMPORARY\\s+)?(?:EXTERNAL\\s+)?TABLE\\b(?:(?!;).)*");
    private static final Pattern NON_HIVE_USING =
            Pattern.compile("(?i)\\bUSING\\s+(?!hive3?\\b)\\w+");
    private static final Pattern HIVE_STORAGE =
            Pattern.compile(
                    "(?i)\\b(?:STORED\\s+AS|STORED\\s+BY|ROW\\s+FORMAT|PARTITIONED\\s+BY|CLUSTERED\\s+BY|SKEWED\\s+BY|TBLPROPERTIES|LOCATION)\\b");
    private static final Pattern CREATE_LIKE =
            Pattern.compile(
                    "(?is)\\bCREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(?:[`\"]?[\\w$]+[`\"]?\\.){0,2}[`\"]?[\\w$]+[`\"]?\\s+LIKE\\s+");
    private static final Pattern COLUMN_LIST =
            Pattern.compile(
                    "(?is)\\bCREATE\\s+(?:TEMPORARY\\s+)?(?:EXTERNAL\\s+)?TABLE\\b(?:(?!;).)*\\([^;]*\\)");
    /** {@code CREATE TABLE t AS SELECT} and {@code CREATE TABLE t AS WITH ... SELECT}. */
    private static final Pattern AS_QUERY = Pattern.compile("(?is)\\bAS\\s+(?:SELECT|WITH)\\b");
    private static final Pattern EXTERNAL_TABLE =
            Pattern.compile("(?is)\\bCREATE\\s+(?:TEMPORARY\\s+)?EXTERNAL\\s+TABLE\\b");

    private HiveTables() {}

    public static boolean isHiveConnector(String connector) {
        if (connector == null) {
            return false;
        }
        String normalized = connector.toLowerCase(Locale.ROOT);
        return "hive".equals(normalized) || "hive3".equals(normalized);
    }

    public static boolean scriptUsesHive(String sql) {
        if (sql == null || sql.isEmpty()) {
            return false;
        }
        if (USING_HIVE.matcher(sql).find()) {
            return true;
        }
        Matcher matcher = CREATE_TABLE_STMT.matcher(sql);
        while (matcher.find()) {
            if (isNativeCreateText(matcher.group())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Text-level check used before the Spark session starts. It mirrors native Hive {@code CREATE
     * TABLE} and ignores connector registrations such as {@code USING jdbc}.
     */
    static boolean isNativeCreateText(String statement) {
        if (statement == null || NON_HIVE_USING.matcher(statement).find()) {
            return false;
        }
        if (HIVE_STORAGE.matcher(statement).find() || CREATE_LIKE.matcher(statement).find()) {
            return true;
        }
        if (EXTERNAL_TABLE.matcher(statement).find()) {
            return true;
        }
        return COLUMN_LIST.matcher(statement).find() && !AS_QUERY.matcher(statement).find();
    }

    /** {@code table_name} on {@code USING hive} must be database.table. */
    public static String requireTableName(Map<String, Object> options) {
        String table = tableName(options);
        String[] parts = table.split("\\.");
        if (parts.length < 2) {
            throw new ParseException("Hive table_name must be database.table, actual: " + table);
        }
        return table;
    }

    /** Table name from DDL or {@code table_name}. One to three parts. */
    public static String tableName(Map<String, Object> options) {
        Object value = options == null ? null : options.get("table_name");
        if (value == null || value.toString().trim().isEmpty()) {
            throw new ParseException("Hive table requires option 'table_name'");
        }
        return writableTable(value.toString());
    }

    public static String writableTable(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new ParseException("Hive table name is empty");
        }
        String table = tableName.trim();
        String[] parts = table.split("\\.");
        if (parts.length < 1 || parts.length > 3) {
            throw new ParseException(
                    "Hive table name must be [catalog.][database.]table, actual: " + table);
        }
        for (String part : parts) {
            if (unquotePart(part).isEmpty()) {
                throw new ParseException(
                        "Hive table name must be [catalog.][database.]table, actual: " + table);
            }
        }
        return table;
    }

    public static String quoteTable(String tableName) {
        String[] parts = writableTable(tableName).split("\\.");
        StringBuilder quoted = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                quoted.append('.');
            }
            String part = unquotePart(parts[i]).replace("`", "``");
            quoted.append('`').append(part).append('`');
        }
        return quoted.toString();
    }

    public static void rejectRowChange(String connector, String operation) {
        if (isHiveConnector(connector)) {
            throw new ParseException(
                    "Hive tables are queried as Spark views. "
                            + operation
                            + " is not supported; use INSERT or TRUNCATE");
        }
    }

    private static String unquotePart(String part) {
        String trimmed = part == null ? "" : part.trim();
        if (trimmed.length() >= 2) {
            char open = trimmed.charAt(0);
            char close = trimmed.charAt(trimmed.length() - 1);
            if ((open == '`' && close == '`') || (open == '"' && close == '"')) {
                return trimmed.substring(1, trimmed.length() - 1);
            }
        }
        return trimmed;
    }
}
