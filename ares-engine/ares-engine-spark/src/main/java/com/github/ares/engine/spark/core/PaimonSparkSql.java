package com.github.ares.engine.spark.core;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;

import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.core.PlParams;
import com.github.ares.parser.paimon.PaimonDml;
import com.github.ares.parser.paimon.PaimonTables;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Read and write Paimon tables through Spark SQL. */
final class PaimonSparkSql {
    private PaimonSparkSql() {}

    static void prepare(SparkSession sparkSession, Map<String, Object> options) {
        if (options == null
                || !PaimonTables.isPaimonConnector(String.valueOf(options.get("connector")))) {
            return;
        }
        if (!PaimonTables.isFilesystem(options)) {
            return;
        }
        String catalog = PaimonTables.catalogName(options);
        String prefix = "spark.sql.catalog." + catalog;
        String warehouse = PaimonTables.warehouse(options);
        setUnlessDifferent(sparkSession, prefix + ".warehouse", warehouse);
        sparkSession.conf().set(prefix, PaimonTables.SPARK_CATALOG_CLASS);
        sparkSession.conf().set(prefix + ".metastore", PaimonTables.FILESYSTEM);
        sparkSession.conf().set(prefix + ".warehouse", warehouse);
        String defaultFs = text(options, PaimonTables.DEFAULT_FS);
        if (defaultFs != null) {
            sparkSession.conf().set("spark.hadoop.fs.defaultFS", defaultFs);
            sparkSession.conf().set(prefix + ".hadoop.fs.defaultFS", defaultFs);
        }
        String hadoopConfDir = PaimonTables.hadoopConfDir(options);
        if (hadoopConfDir != null) {
            sparkSession.conf().set(prefix + ".hadoop-conf-dir", hadoopConfDir);
        }
        for (Map.Entry<String, Object> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("fs.") || key.startsWith("dfs.") || key.startsWith("hadoop.")) {
                String value = String.valueOf(entry.getValue());
                sparkSession.conf().set("spark.hadoop." + key, value);
                String catalogKey =
                        key.startsWith("hadoop.") ? prefix + "." + key : prefix + ".hadoop." + key;
                sparkSession.conf().set(catalogKey, value);
            }
        }
    }

    static void create(SparkSession sparkSession, Map<String, Object> options, String sql) {
        prepare(sparkSession, options);
        sparkSession.sql(sql);
    }

    static void insert(
            SparkSession sparkSession,
            Map<String, Object> options,
            Dataset<Row> data,
            List<String> targetColumns) {
        prepare(sparkSession, options);
        HiveSparkSql.insert(
                sparkSession, PaimonTables.qualifiedTable(options), data, targetColumns);
    }

    static void truncate(SparkSession sparkSession, Map<String, Object> options) {
        prepare(sparkSession, options);
        HiveSparkSql.truncate(sparkSession, PaimonTables.qualifiedTable(options));
    }

    static void executeDml(
            SparkSession sparkSession,
            Map<String, Object> options,
            String paimonSql,
            PlParams plParams,
            String viewName) {
        prepare(sparkSession, options);
        String quotedTable = PaimonTables.quoteTable(PaimonTables.qualifiedTable(options));
        String sql = replaceParams(paimonSql, plParams).replace(PaimonDml.TARGET, quotedTable);
        sparkSession.sql(sql);
        refreshView(sparkSession, viewName, quotedTable);
    }

    private static void setUnlessDifferent(SparkSession sparkSession, String key, String value) {
        try {
            String existing = sparkSession.conf().get(key);
            if (!existing.equals(value)) {
                throw new AresException(
                        "Paimon catalog is already configured with "
                                + key
                                + "="
                                + existing
                                + ". Use another 'catalog' name for warehouse "
                                + value);
            }
        } catch (NoSuchElementException ignored) {
            // Catalog warehouse is not configured yet.
        }
    }

    private static void refreshView(
            SparkSession sparkSession, String viewName, String quotedTable) {
        if (viewName == null || viewName.trim().isEmpty()) {
            return;
        }
        try {
            org.apache.spark.sql.catalog.Table table = sparkSession.catalog().getTable(viewName);
            if (!table.isTemporary()) {
                return;
            }
        } catch (Exception ignored) {
            return;
        }
        sparkSession.sql("SELECT * FROM " + quotedTable).createOrReplaceTempView(viewName);
    }

    private static String text(Map<String, Object> options, String key) {
        Object value = options.get(key);
        if (value == null) {
            return null;
        }
        String text = value.toString().trim();
        return text.isEmpty() ? null : text;
    }
}
