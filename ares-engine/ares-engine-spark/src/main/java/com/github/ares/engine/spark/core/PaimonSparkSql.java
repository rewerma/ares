package com.github.ares.engine.spark.core;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;

import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.core.PlParams;
import com.github.ares.parser.lake.LakeFormat;
import com.github.ares.parser.lake.LakeTables;
import com.github.ares.parser.paimon.PaimonDml;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Read and write Paimon, Hudi, and Iceberg tables through Spark SQL. */
final class PaimonSparkSql {
    private PaimonSparkSql() {}

    static void prepare(SparkSession sparkSession, Map<String, Object> options) {
        configure(sparkSession, options);
        ensureHudiTable(sparkSession, options);
    }

    static void create(SparkSession sparkSession, Map<String, Object> options, String sql) {
        configure(sparkSession, options);
        sparkSession.sql(sql);
        String identifierFields =
                options == null ? null : text(options, LakeTables.IDENTIFIER_FIELDS);
        if (identifierFields == null || LakeTables.requireFormat(options) != LakeFormat.ICEBERG) {
            return;
        }
        String quotedTable = LakeTables.quoteTable(LakeTables.qualifiedTable(options));
        sparkSession.sql(
                "ALTER TABLE "
                        + quotedTable
                        + " SET IDENTIFIER FIELDS "
                        + quoteFields(identifierFields));
    }

    static void insert(
            SparkSession sparkSession,
            Map<String, Object> options,
            Dataset<Row> data,
            List<String> targetColumns) {
        prepare(sparkSession, options);
        HiveSparkSql.insert(sparkSession, LakeTables.qualifiedTable(options), data, targetColumns);
    }

    static void truncate(SparkSession sparkSession, Map<String, Object> options) {
        prepare(sparkSession, options);
        HiveSparkSql.truncate(sparkSession, LakeTables.qualifiedTable(options));
    }

    static void executeDml(
            SparkSession sparkSession,
            Map<String, Object> options,
            String lakeSql,
            PlParams plParams,
            String viewName) {
        prepare(sparkSession, options);
        String quotedTable = LakeTables.quoteTable(LakeTables.qualifiedTable(options));
        String sql = replaceParams(lakeSql, plParams).replace(PaimonDml.TARGET, quotedTable);
        sparkSession.sql(sql);
        refreshView(sparkSession, viewName, quotedTable);
    }

    private static void configure(SparkSession sparkSession, Map<String, Object> options) {
        if (options == null
                || !LakeTables.isLakeConnector(String.valueOf(options.get("connector")))) {
            return;
        }
        if (!LakeTables.isFilesystem(options)) {
            return;
        }
        LakeFormat format = LakeTables.requireFormat(options);
        String warehouse = LakeTables.warehouse(options);
        if (!format.namedCatalog()) {
            setUnlessDifferent(
                    sparkSession,
                    format,
                    "spark.sql.catalog.spark_catalog",
                    format.catalogClass(),
                    warehouse);
            sparkSession.conf().set("spark.sql.catalog.spark_catalog", format.catalogClass());
            applyHadoop(sparkSession, options, null);
            return;
        }
        String catalog = LakeTables.catalogName(options);
        String prefix = "spark.sql.catalog." + catalog;
        setUnlessDifferent(sparkSession, format, prefix + ".warehouse", warehouse, warehouse);
        sparkSession.conf().set(prefix, format.catalogClass());
        if (format == LakeFormat.ICEBERG) {
            sparkSession.conf().set(prefix + ".type", "hadoop");
        } else {
            sparkSession.conf().set(prefix + ".metastore", LakeTables.FILESYSTEM);
        }
        sparkSession.conf().set(prefix + ".warehouse", warehouse);
        applyHadoop(sparkSession, options, prefix);
    }

    private static void ensureHudiTable(SparkSession sparkSession, Map<String, Object> options) {
        if (options == null
                || LakeFormat.of(String.valueOf(options.get("connector"))) != LakeFormat.HUDI) {
            return;
        }
        if (!LakeTables.isFilesystem(options)) {
            return;
        }
        configure(sparkSession, options);
        String table = LakeTables.requireTableName(options);
        String database = table.split("\\.")[0].trim();
        sparkSession.sql("CREATE DATABASE IF NOT EXISTS " + LakeTables.quoteIdent(database));
        String location = LakeTables.location(options).replace("'", "''");
        sparkSession.sql(
                "CREATE TABLE IF NOT EXISTS "
                        + LakeTables.quoteTable(table)
                        + " USING hudi LOCATION '"
                        + location
                        + "'");
    }

    private static void applyHadoop(
            SparkSession sparkSession, Map<String, Object> options, String catalogPrefix) {
        String defaultFs = text(options, LakeTables.DEFAULT_FS);
        if (defaultFs != null) {
            sparkSession.conf().set("spark.hadoop.fs.defaultFS", defaultFs);
            if (catalogPrefix != null) {
                sparkSession.conf().set(catalogPrefix + ".hadoop.fs.defaultFS", defaultFs);
            }
        }
        if (catalogPrefix != null) {
            String hadoopConfDir = LakeTables.hadoopConfDir(options);
            if (hadoopConfDir != null) {
                sparkSession.conf().set(catalogPrefix + ".hadoop-conf-dir", hadoopConfDir);
            }
        }
        for (Map.Entry<String, Object> entry : options.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith("fs.") && !key.startsWith("dfs.") && !key.startsWith("hadoop.")) {
                continue;
            }
            String value = String.valueOf(entry.getValue());
            sparkSession.conf().set("spark.hadoop." + key, value);
            if (catalogPrefix == null) {
                continue;
            }
            String catalogKey =
                    key.startsWith("hadoop.")
                            ? catalogPrefix + "." + key
                            : catalogPrefix + ".hadoop." + key;
            sparkSession.conf().set(catalogKey, value);
        }
    }

    private static String quoteFields(String fields) {
        String[] parts = fields.split(",");
        StringBuilder quoted = new StringBuilder();
        for (String part : parts) {
            String name = part.trim();
            if (name.isEmpty()) {
                continue;
            }
            if (quoted.length() > 0) {
                quoted.append(", ");
            }
            quoted.append(LakeTables.quoteIdent(name));
        }
        return quoted.toString();
    }

    private static void setUnlessDifferent(
            SparkSession sparkSession,
            LakeFormat format,
            String key,
            String value,
            String warehouse) {
        try {
            String existing = sparkSession.conf().get(key);
            if (!existing.equals(value)) {
                throw new AresException(
                        format.displayName()
                                + " catalog is already configured with "
                                + key
                                + "="
                                + existing
                                + ". Use another 'catalog' name for warehouse "
                                + warehouse);
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
