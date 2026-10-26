package com.github.ares.engine.spark.core;

import com.github.ares.parser.hive.HiveTables;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Read and write Hive tables through the Spark session that already has Hive enabled. */
final class HiveSparkSql {
    private static final String WRITE_SRC = "__ares_hive_write_src";

    private HiveSparkSql() {}

    static void insert(
            SparkSession sparkSession,
            String hiveTable,
            Dataset<Row> data,
            List<String> targetColumns) {
        String quotedTable = HiveTables.quoteTable(HiveTables.writableTable(hiveTable));
        data.createOrReplaceTempView(WRITE_SRC);
        try {
            String sql;
            if (targetColumns == null || targetColumns.isEmpty()) {
                sql = "INSERT INTO " + quotedTable + " SELECT * FROM " + WRITE_SRC;
            } else {
                String columns =
                        targetColumns.stream()
                                .map(column -> "`" + column.replace("`", "``") + "`")
                                .collect(Collectors.joining(", "));
                sql =
                        "INSERT INTO "
                                + quotedTable
                                + " ("
                                + columns
                                + ") SELECT * FROM "
                                + WRITE_SRC;
            }
            sparkSession.sql(sql);
        } finally {
            sparkSession.catalog().dropTempView(WRITE_SRC);
        }
    }

    static void truncate(SparkSession sparkSession, String hiveTable) {
        sparkSession.sql(
                "TRUNCATE TABLE " + HiveTables.quoteTable(HiveTables.writableTable(hiveTable)));
    }

    static void create(SparkSession sparkSession, String sql) {
        sparkSession.sql(sql);
    }
}
