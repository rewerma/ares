package com.github.ares.parser.paimon;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.plan.LogicalCreatePaimonTable;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalCreateTableAsSQL;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalProject;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Paimon tables are Spark catalog tables. Hive metastore uses the same path as Hive. Filesystem
 * metastore registers a Paimon catalog, including an HDFS warehouse.
 */
public final class PaimonTables {
    public static final String CONNECTOR = "paimon";
    public static final String METASTORE = "metastore";
    public static final String HIVE = "hive";
    public static final String FILESYSTEM = "filesystem";
    public static final String WAREHOUSE = "warehouse";
    public static final String DEFAULT_FS = "fs.defaultFS";
    public static final String HDFS_SITE_PATH = "hdfs_site_path";
    public static final String CATALOG = "catalog";
    public static final String DEFAULT_CATALOG = "paimon";
    public static final String SPARK_CATALOG_CLASS = "org.apache.paimon.spark.SparkCatalog";
    public static final String SPARK_EXTENSIONS_CLASS =
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions";

    private static final Pattern USING_PAIMON = Pattern.compile("(?i)\\bUSING\\s+paimon\\b");
    private static final Pattern HIVE_METASTORE =
            Pattern.compile("(?i)['\"]metastore['\"]\\s*=\\s*['\"]hive['\"]");
    private static final Pattern FILESYSTEM_METASTORE =
            Pattern.compile("(?i)['\"]metastore['\"]\\s*=\\s*['\"]filesystem['\"]");
    private static final Pattern CATALOG_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private PaimonTables() {}

    public static boolean isPaimonConnector(String connector) {
        return connector != null && CONNECTOR.equals(connector.toLowerCase(Locale.ROOT));
    }

    public static boolean scriptUsesPaimon(String sql) {
        return sql != null && USING_PAIMON.matcher(sql).find();
    }

    public static boolean scriptUsesHiveMetastore(String sql) {
        return scriptUsesPaimon(sql) && HIVE_METASTORE.matcher(sql).find();
    }

    public static boolean scriptUsesFilesystem(String sql) {
        return scriptUsesPaimon(sql) && FILESYSTEM_METASTORE.matcher(sql).find();
    }

    public static boolean isHiveMetastore(Map<String, Object> options) {
        return HIVE.equals(metastore(options));
    }

    public static boolean isFilesystem(Map<String, Object> options) {
        return FILESYSTEM.equals(metastore(options));
    }

    public static String metastore(Map<String, Object> options) {
        Object value = options == null ? null : options.get(METASTORE);
        if (value == null || value.toString().trim().isEmpty()) {
            throw new ParseException(
                    "Paimon table requires option 'metastore' as 'filesystem' or 'hive'");
        }
        String metastore = value.toString().trim().toLowerCase(Locale.ROOT);
        if (!HIVE.equals(metastore) && !FILESYSTEM.equals(metastore)) {
            throw new ParseException(
                    "Paimon metastore must be 'filesystem' or 'hive', actual: " + value);
        }
        return metastore;
    }

    /** Options stored on the sink table and on the source view. */
    public static Map<String, Object> connectorOptions(Map<String, Object> options) {
        String metastore = metastore(options);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("connector", CONNECTOR);
        result.put(METASTORE, metastore);
        result.put("table_name", requireTableName(options, metastore));
        if (FILESYSTEM.equals(metastore)) {
            result.put(WAREHOUSE, warehouse(options));
            copyIfPresent(result, options, DEFAULT_FS);
            copyIfPresent(result, options, HDFS_SITE_PATH);
            copyIfPresent(result, options, CATALOG);
            copyHadoopKeys(result, options);
        }
        return result;
    }

    public static String requireTableName(Map<String, Object> options) {
        return requireTableName(options, metastore(options));
    }

    public static String catalogName(Map<String, Object> options) {
        Object value = options == null ? null : options.get(CATALOG);
        if (value == null || value.toString().trim().isEmpty()) {
            return DEFAULT_CATALOG;
        }
        String name = value.toString().trim();
        if (!CATALOG_NAME.matcher(name).matches() || "spark_catalog".equalsIgnoreCase(name)) {
            throw new ParseException(
                    "Paimon filesystem catalog must be an identifier other than spark_catalog, actual: "
                            + name);
        }
        return name;
    }

    /**
     * Hive metastore keeps {@code database.table}. Filesystem reads {@code catalog.database.table}.
     */
    public static String qualifiedTable(Map<String, Object> options) {
        String table = requireTableName(options);
        if (isHiveMetastore(options)) {
            return table;
        }
        return catalogName(options) + "." + table;
    }

    public static String quoteTable(String tableName) {
        String[] parts = tableName.split("\\.");
        StringBuilder quoted = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                quoted.append('.');
            }
            quoted.append('`').append(parts[i].trim().replace("`", "``")).append('`');
        }
        return quoted.toString();
    }

    public static String warehouse(Map<String, Object> options) {
        Object value = options == null ? null : options.get(WAREHOUSE);
        if (value == null || value.toString().trim().isEmpty()) {
            throw new ParseException(
                    "Paimon filesystem catalog requires option 'warehouse', "
                            + "for example hdfs://localhost:9000/paimon or an HDFS path with 'fs.defaultFS'");
        }
        String warehouse = value.toString().trim();
        String defaultFs = text(options, DEFAULT_FS);
        if (defaultFs != null && !warehouse.contains("://")) {
            if (defaultFs.endsWith("/")) {
                defaultFs = defaultFs.substring(0, defaultFs.length() - 1);
            }
            if (!warehouse.startsWith("/")) {
                warehouse = "/" + warehouse;
            }
            return defaultFs + warehouse;
        }
        return warehouse;
    }

    public static String hadoopConfDir(Map<String, Object> options) {
        String path = text(options, HDFS_SITE_PATH);
        if (path == null) {
            return null;
        }
        if (path.endsWith(".xml")) {
            int slash = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
            if (slash > 0) {
                return path.substring(0, slash);
            }
        }
        return path;
    }

    public static boolean usesHdfs(Map<String, Object> options) {
        if (options == null || !isFilesystem(options)) {
            return false;
        }
        String warehouse = text(options, WAREHOUSE);
        String defaultFs = text(options, DEFAULT_FS);
        return startsWithHdfs(warehouse) || startsWithHdfs(defaultFs);
    }

    public static boolean projectUsesFilesystem(LogicalProject project) {
        boolean[] found = new boolean[1];
        eachPaimonOptions(
                project,
                options -> {
                    if (isFilesystem(options)) {
                        found[0] = true;
                    }
                });
        return found[0];
    }

    public static boolean projectUsesHdfs(LogicalProject project) {
        boolean[] found = new boolean[1];
        eachPaimonOptions(
                project,
                options -> {
                    if (usesHdfs(options)) {
                        found[0] = true;
                    }
                });
        return found[0];
    }

    private static String requireTableName(Map<String, Object> options, String metastore) {
        Object value = options == null ? null : options.get("table_name");
        if (value == null || value.toString().trim().isEmpty()) {
            throw new ParseException("Paimon table requires option 'table_name' as database.table");
        }
        String table = value.toString().trim();
        String[] parts = table.split("\\.");
        int maxParts = HIVE.equals(metastore) ? 3 : 2;
        if (parts.length < 2 || parts.length > maxParts) {
            throw new ParseException("Paimon table_name must be database.table, actual: " + table);
        }
        for (String part : parts) {
            if (part.trim().isEmpty()) {
                throw new ParseException(
                        "Paimon table_name must be database.table, actual: " + table);
            }
        }
        return table;
    }

    private static void eachPaimonOptions(
            LogicalProject project, Consumer<Map<String, Object>> consumer) {
        if (project == null || project.getLogicalOperations() == null) {
            return;
        }
        for (LogicalOperation operation : project.getLogicalOperations()) {
            if (operation instanceof LogicalCreateSinkTable) {
                LogicalCreateSinkTable sink = (LogicalCreateSinkTable) operation;
                if (isPaimonConnector(sink.getConnector())) {
                    consumer.accept(sink.getOptions());
                }
            } else if (operation instanceof LogicalCreatePaimonTable) {
                Map<String, Object> properties =
                        ((LogicalCreatePaimonTable) operation).getProperties();
                if (properties != null
                        && isPaimonConnector(String.valueOf(properties.get("connector")))) {
                    consumer.accept(properties);
                }
            } else if (operation instanceof LogicalCreateTableAsSQL) {
                Map<String, Object> properties =
                        ((LogicalCreateTableAsSQL) operation).getProperties();
                if (properties != null
                        && isPaimonConnector(String.valueOf(properties.get("connector")))) {
                    consumer.accept(properties);
                }
            }
        }
    }

    private static void copyIfPresent(
            Map<String, Object> target, Map<String, Object> source, String key) {
        if (source != null
                && source.get(key) != null
                && !source.get(key).toString().trim().isEmpty()) {
            target.put(key, source.get(key).toString().trim());
        }
    }

    private static void copyHadoopKeys(Map<String, Object> target, Map<String, Object> source) {
        if (source == null) {
            return;
        }
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("fs.") || key.startsWith("dfs.") || key.startsWith("hadoop.")) {
                target.put(key, entry.getValue());
            }
        }
    }

    private static String text(Map<String, Object> options, String key) {
        if (options == null || options.get(key) == null) {
            return null;
        }
        String value = options.get(key).toString().trim();
        return value.isEmpty() ? null : value;
    }

    private static boolean startsWithHdfs(String value) {
        return value != null && value.toLowerCase(Locale.ROOT).startsWith("hdfs:");
    }
}
