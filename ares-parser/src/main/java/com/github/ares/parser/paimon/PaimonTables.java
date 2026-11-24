package com.github.ares.parser.paimon;

import com.github.ares.parser.lake.LakeFormat;
import com.github.ares.parser.lake.LakeTables;
import com.github.ares.parser.plan.LogicalProject;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Paimon tables are Spark catalog tables. Hive metastore uses the same path as Hive. Filesystem
 * metastore registers a Paimon catalog, including an HDFS warehouse.
 */
public final class PaimonTables {
    public static final String CONNECTOR = LakeFormat.PAIMON.connector();
    public static final String METASTORE = LakeTables.METASTORE;
    public static final String HIVE = LakeTables.HIVE;
    public static final String FILESYSTEM = LakeTables.FILESYSTEM;
    public static final String WAREHOUSE = LakeTables.WAREHOUSE;
    public static final String DEFAULT_FS = LakeTables.DEFAULT_FS;
    public static final String HDFS_SITE_PATH = LakeTables.HDFS_SITE_PATH;
    public static final String CATALOG = LakeTables.CATALOG;
    public static final String DEFAULT_CATALOG = LakeFormat.PAIMON.defaultCatalog();
    public static final String SPARK_CATALOG_CLASS = LakeFormat.PAIMON.catalogClass();
    public static final String SPARK_EXTENSIONS_CLASS = LakeFormat.PAIMON.extensionsClass();

    private static final Pattern USING_PAIMON = Pattern.compile("(?i)\\bUSING\\s+paimon\\b");

    private PaimonTables() {}

    public static boolean isPaimonConnector(String connector) {
        return LakeFormat.PAIMON == LakeFormat.of(connector);
    }

    public static boolean scriptUsesPaimon(String sql) {
        return sql != null && USING_PAIMON.matcher(sql).find();
    }

    public static boolean scriptUsesHiveMetastore(String sql) {
        return scriptUsesPaimon(sql) && LakeTables.scriptUsesHiveMetastore(sql);
    }

    public static boolean scriptUsesFilesystem(String sql) {
        return LakeTables.scriptUsesFilesystem(sql, LakeFormat.PAIMON);
    }

    public static boolean isHiveMetastore(Map<String, Object> options) {
        return LakeTables.HIVE.equals(metastore(options));
    }

    public static boolean isFilesystem(Map<String, Object> options) {
        return LakeTables.FILESYSTEM.equals(metastore(options));
    }

    public static String metastore(Map<String, Object> options) {
        return LakeTables.metastore(LakeFormat.PAIMON, options);
    }

    public static Map<String, Object> connectorOptions(Map<String, Object> options) {
        return LakeTables.connectorOptions(withConnector(options));
    }

    public static String requireTableName(Map<String, Object> options) {
        return LakeTables.requireTableName(withConnector(options));
    }

    public static String catalogName(Map<String, Object> options) {
        return LakeTables.catalogName(withConnector(options));
    }

    public static String qualifiedTable(Map<String, Object> options) {
        return LakeTables.qualifiedTable(withConnector(options));
    }

    public static String quoteTable(String tableName) {
        return LakeTables.quoteTable(tableName);
    }

    public static String warehouse(Map<String, Object> options) {
        return LakeTables.warehouse(withConnector(options));
    }

    public static String hadoopConfDir(Map<String, Object> options) {
        return LakeTables.hadoopConfDir(options);
    }

    public static boolean usesHdfs(Map<String, Object> options) {
        return LakeTables.usesHdfs(withConnector(options));
    }

    public static boolean projectUsesFilesystem(LogicalProject project) {
        return LakeTables.projectUsesFilesystem(project, LakeFormat.PAIMON);
    }

    public static boolean projectUsesHdfs(LogicalProject project) {
        return LakeTables.projectUsesHdfs(project, LakeFormat.PAIMON);
    }

    /** Keep Paimon error text when older callers omit the connector key. */
    private static Map<String, Object> withConnector(Map<String, Object> options) {
        if (options != null && options.get("connector") != null) {
            return options;
        }
        java.util.Map<String, Object> copy = new java.util.LinkedHashMap<>();
        if (options != null) {
            copy.putAll(options);
        }
        copy.put("connector", CONNECTOR);
        return copy;
    }
}
