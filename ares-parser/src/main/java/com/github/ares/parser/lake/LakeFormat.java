package com.github.ares.parser.lake;

import java.util.Locale;
import java.util.regex.Pattern;

/** Spark lake formats that share one metastore option style. */
public enum LakeFormat {
    PAIMON(
            "paimon",
            "Paimon",
            "org.apache.paimon.spark.SparkCatalog",
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
            "paimon",
            "primary-key",
            true),
    HUDI(
            "hudi",
            "Hudi",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
            "spark_catalog",
            "primaryKey",
            false),
    ICEBERG(
            "iceberg",
            "Iceberg",
            "org.apache.iceberg.spark.SparkCatalog",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "iceberg",
            null,
            true);

    private final String connector;
    private final String displayName;
    private final String catalogClass;
    private final String extensionsClass;
    private final String defaultCatalog;
    /** Table property for {@code PRIMARY KEY}. Null sets Iceberg identifier fields instead. */
    private final String primaryKeyProperty;
    /** False when the format must use the Spark session catalog. */
    private final boolean namedCatalog;

    private final Pattern usingPattern;

    LakeFormat(
            String connector,
            String displayName,
            String catalogClass,
            String extensionsClass,
            String defaultCatalog,
            String primaryKeyProperty,
            boolean namedCatalog) {
        this.connector = connector;
        this.displayName = displayName;
        this.catalogClass = catalogClass;
        this.extensionsClass = extensionsClass;
        this.defaultCatalog = defaultCatalog;
        this.primaryKeyProperty = primaryKeyProperty;
        this.namedCatalog = namedCatalog;
        this.usingPattern = Pattern.compile("(?i)\\bUSING\\s+" + connector + "\\b");
    }

    public String connector() {
        return connector;
    }

    public String displayName() {
        return displayName;
    }

    public String catalogClass() {
        return catalogClass;
    }

    public String extensionsClass() {
        return extensionsClass;
    }

    public String defaultCatalog() {
        return defaultCatalog;
    }

    public String primaryKeyProperty() {
        return primaryKeyProperty;
    }

    public boolean namedCatalog() {
        return namedCatalog;
    }

    public boolean uses(String sql) {
        return sql != null && usingPattern.matcher(sql).find();
    }

    public static LakeFormat of(String connector) {
        if (connector == null) {
            return null;
        }
        String normalized = connector.trim().toLowerCase(Locale.ROOT);
        for (LakeFormat format : values()) {
            if (format.connector.equals(normalized)) {
                return format;
            }
        }
        return null;
    }

    /** First {@code USING} clause in the script, when it names a lake format. */
    public static LakeFormat matching(String sql) {
        if (sql == null) {
            return null;
        }
        LakeFormat found = null;
        int index = Integer.MAX_VALUE;
        for (LakeFormat format : values()) {
            java.util.regex.Matcher matcher = format.usingPattern.matcher(sql);
            if (matcher.find() && matcher.start() < index) {
                index = matcher.start();
                found = format;
            }
        }
        return found;
    }
}
