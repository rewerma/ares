package com.github.ares.parser.lake;

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
 * Paimon, Hudi, and Iceberg tables are Spark catalog tables. Hive metastore uses the cluster
 * catalog. Filesystem metastore registers a catalog, including an HDFS warehouse. Hudi filesystem
 * tables live in the Spark session catalog and take a warehouse location.
 */
public final class LakeTables {
    public static final String METASTORE = "metastore";
    public static final String HIVE = "hive";
    public static final String FILESYSTEM = "filesystem";
    public static final String WAREHOUSE = "warehouse";
    public static final String DEFAULT_FS = "fs.defaultFS";
    public static final String HDFS_SITE_PATH = "hdfs_site_path";
    public static final String CATALOG = "catalog";
    public static final String IDENTIFIER_FIELDS = "identifier-fields";

    private static final Pattern HIVE_METASTORE =
            Pattern.compile("(?i)['\"]metastore['\"]\\s*=\\s*['\"]hive['\"]");
    private static final Pattern FILESYSTEM_METASTORE =
            Pattern.compile("(?i)['\"]metastore['\"]\\s*=\\s*['\"]filesystem['\"]");
    private static final Pattern CATALOG_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private LakeTables() {}

    public static boolean isLakeConnector(String connector) {
        return LakeFormat.of(connector) != null;
    }

    public static LakeFormat requireFormat(Map<String, Object> options) {
        Object connector = options == null ? null : options.get("connector");
        LakeFormat format = LakeFormat.of(connector == null ? null : connector.toString());
        if (format == null) {
            throw new ParseException("Lake table connector is missing");
        }
        return format;
    }

    public static boolean scriptUsesLake(String sql) {
        return LakeFormat.matching(sql) != null;
    }

    public static boolean scriptUsesHiveMetastore(String sql) {
        if (sql == null || !HIVE_METASTORE.matcher(sql).find()) {
            return false;
        }
        for (LakeFormat format : LakeFormat.values()) {
            if (format.uses(sql)) {
                return true;
            }
        }
        return false;
    }

    public static boolean scriptUsesFilesystem(String sql) {
        return scriptUsesFilesystem(sql, null);
    }

    public static boolean scriptUsesFilesystem(String sql, LakeFormat format) {
        if (sql == null || !FILESYSTEM_METASTORE.matcher(sql).find()) {
            return false;
        }
        if (format != null) {
            return format.uses(sql);
        }
        return scriptUsesLake(sql);
    }

    public static boolean isHiveMetastore(Map<String, Object> options) {
        return HIVE.equals(metastore(options));
    }

    public static boolean isFilesystem(Map<String, Object> options) {
        return FILESYSTEM.equals(metastore(options));
    }

    public static String metastore(Map<String, Object> options) {
        return metastore(label(options), options);
    }

    public static String metastore(LakeFormat format, Map<String, Object> options) {
        Object value = options == null ? null : options.get(METASTORE);
        if (value == null || value.toString().trim().isEmpty()) {
            throw new ParseException(
                    format.displayName()
                            + " table requires option 'metastore' as 'filesystem' or 'hive'");
        }
        String metastore = value.toString().trim().toLowerCase(Locale.ROOT);
        if (!HIVE.equals(metastore) && !FILESYSTEM.equals(metastore)) {
            throw new ParseException(
                    format.displayName()
                            + " metastore must be 'filesystem' or 'hive', actual: "
                            + value);
        }
        return metastore;
    }

    /** Options stored on the sink table and on the source view. */
    public static Map<String, Object> connectorOptions(Map<String, Object> options) {
        LakeFormat format = requireFormat(options);
        String metastore = metastore(format, options);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("connector", format.connector());
        result.put(METASTORE, metastore);
        result.put("table_name", requireTableName(format, options, metastore));
        if (FILESYSTEM.equals(metastore)) {
            result.put(WAREHOUSE, warehouse(format, options));
            catalogName(format, options);
            copyIfPresent(result, options, DEFAULT_FS);
            copyIfPresent(result, options, HDFS_SITE_PATH);
            copyIfPresent(result, options, CATALOG);
            copyHadoopKeys(result, options);
        }
        return result;
    }

    public static String requireTableName(Map<String, Object> options) {
        return requireTableName(label(options), options, metastore(options));
    }

    public static String catalogName(Map<String, Object> options) {
        return catalogName(label(options), options);
    }

    /**
     * Hive metastore keeps {@code database.table}. Named filesystem catalogs read {@code
     * catalog.database.table}. Hudi filesystem tables stay {@code database.table} on the session
     * catalog.
     */
    public static String qualifiedTable(Map<String, Object> options) {
        LakeFormat format = label(options);
        String table = requireTableName(format, options, metastore(format, options));
        if (isHiveMetastore(options) || !format.namedCatalog()) {
            return table;
        }
        return catalogName(format, options) + "." + table;
    }

    /** Warehouse path of {@code database/table} for a filesystem table. */
    public static String location(Map<String, Object> options) {
        LakeFormat format = label(options);
        String warehouse = warehouse(format, options);
        while (warehouse.endsWith("/")) {
            warehouse = warehouse.substring(0, warehouse.length() - 1);
        }
        String table = requireTableName(format, options, metastore(format, options));
        return warehouse + "/" + table.replace('.', '/');
    }

    public static String quoteTable(String tableName) {
        String[] parts = tableName.split("\\.");
        StringBuilder quoted = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                quoted.append('.');
            }
            quoted.append(quoteIdent(parts[i].trim()));
        }
        return quoted.toString();
    }

    public static String quoteIdent(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    public static String warehouse(Map<String, Object> options) {
        return warehouse(label(options), options);
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
        return projectUsesFilesystem(project, null);
    }

    public static boolean projectUsesFilesystem(LogicalProject project, LakeFormat format) {
        boolean[] found = new boolean[1];
        eachLakeOptions(
                project,
                format,
                options -> {
                    if (isFilesystem(options)) {
                        found[0] = true;
                    }
                });
        return found[0];
    }

    public static boolean projectUsesHdfs(LogicalProject project) {
        return projectUsesHdfs(project, null);
    }

    public static boolean projectUsesHdfs(LogicalProject project, LakeFormat format) {
        boolean[] found = new boolean[1];
        eachLakeOptions(
                project,
                format,
                options -> {
                    if (usesHdfs(options)) {
                        found[0] = true;
                    }
                });
        return found[0];
    }

    private static String requireTableName(
            LakeFormat format, Map<String, Object> options, String metastore) {
        Object value = options == null ? null : options.get("table_name");
        if (value == null || value.toString().trim().isEmpty()) {
            throw new ParseException(
                    format.displayName() + " table requires option 'table_name' as database.table");
        }
        String table = value.toString().trim();
        String[] parts = table.split("\\.");
        int maxParts = HIVE.equals(metastore) ? 3 : 2;
        if (parts.length < 2 || parts.length > maxParts) {
            throw new ParseException(
                    format.displayName() + " table_name must be database.table, actual: " + table);
        }
        for (String part : parts) {
            if (part.trim().isEmpty()) {
                throw new ParseException(
                        format.displayName()
                                + " table_name must be database.table, actual: "
                                + table);
            }
        }
        return table;
    }

    private static String catalogName(LakeFormat format, Map<String, Object> options) {
        Object value = options == null ? null : options.get(CATALOG);
        if (!format.namedCatalog()) {
            if (value == null || value.toString().trim().isEmpty()) {
                return format.defaultCatalog();
            }
            String name = value.toString().trim();
            if ("spark_catalog".equalsIgnoreCase(name)
                    || format.connector().equalsIgnoreCase(name)) {
                return format.defaultCatalog();
            }
            throw new ParseException(
                    format.displayName()
                            + " filesystem catalog uses the Spark session catalog. Remove 'catalog'"
                            + " or set it to spark_catalog, actual: "
                            + name);
        }
        if (value == null || value.toString().trim().isEmpty()) {
            return format.defaultCatalog();
        }
        String name = value.toString().trim();
        if (!CATALOG_NAME.matcher(name).matches() || "spark_catalog".equalsIgnoreCase(name)) {
            throw new ParseException(
                    format.displayName()
                            + " filesystem catalog must be an identifier other than spark_catalog,"
                            + " actual: "
                            + name);
        }
        return name;
    }

    private static String warehouse(LakeFormat format, Map<String, Object> options) {
        Object value = options == null ? null : options.get(WAREHOUSE);
        if (value == null || value.toString().trim().isEmpty()) {
            throw new ParseException(
                    format.displayName()
                            + " filesystem catalog requires option 'warehouse', "
                            + "for example hdfs://localhost:9000/"
                            + format.connector()
                            + " or an HDFS path with 'fs.defaultFS'");
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

    private static void eachLakeOptions(
            LogicalProject project, LakeFormat format, Consumer<Map<String, Object>> consumer) {
        if (project == null || project.getLogicalOperations() == null) {
            return;
        }
        for (LogicalOperation operation : project.getLogicalOperations()) {
            if (operation instanceof LogicalCreateSinkTable) {
                LogicalCreateSinkTable sink = (LogicalCreateSinkTable) operation;
                if (matches(format, sink.getConnector())) {
                    consumer.accept(sink.getOptions());
                }
            } else if (operation instanceof LogicalCreatePaimonTable) {
                acceptProperties(
                        format, ((LogicalCreatePaimonTable) operation).getProperties(), consumer);
            } else if (operation instanceof LogicalCreateTableAsSQL) {
                acceptProperties(
                        format, ((LogicalCreateTableAsSQL) operation).getProperties(), consumer);
            }
        }
    }

    private static void acceptProperties(
            LakeFormat format,
            Map<String, Object> properties,
            Consumer<Map<String, Object>> consumer) {
        if (properties == null) {
            return;
        }
        Object connector = properties.get("connector");
        if (connector != null && matches(format, connector.toString())) {
            consumer.accept(properties);
        }
    }

    private static boolean matches(LakeFormat format, String connector) {
        if (format == null) {
            return isLakeConnector(connector);
        }
        return format == LakeFormat.of(connector);
    }

    private static LakeFormat label(Map<String, Object> options) {
        Object connector = options == null ? null : options.get("connector");
        LakeFormat format = LakeFormat.of(connector == null ? null : connector.toString());
        return format == null ? LakeFormat.PAIMON : format;
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
