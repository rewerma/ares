package com.github.ares.connctor.jdbc.internal.dialect.sqlserver;

import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.connctor.jdbc.internal.converter.JdbcRowConverter;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectTypeMapper;
import com.github.ares.connctor.jdbc.internal.dialect.SQLUtils;
import com.github.ares.connctor.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import com.github.ares.connctor.jdbc.source.JdbcSourceTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class SqlServerDialect implements JdbcDialect {

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public SqlServerDialect() {}

    public SqlServerDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.SQLSERVER;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new SqlserverJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new SqlserverTypeMapper();
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return "ABS(HASHBYTES('MD5', " + quoteIdentifier(fieldName) + ") % " + mod + ")";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        List<String> nonUniqueKeyFields =
                Arrays.stream(fieldNames)
                        .filter(fieldName -> !Arrays.asList(uniqueKeyFields).contains(fieldName))
                        .collect(Collectors.toList());
        String valuesBinding =
                Arrays.stream(fieldNames)
                        .map(fieldName -> ":" + fieldName + " " + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));

        String usingClause = String.format("SELECT %s", valuesBinding);
        String onConditions =
                Arrays.stream(uniqueKeyFields)
                        .map(
                                fieldName ->
                                        String.format(
                                                "[TARGET].%s=[SOURCE].%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(" AND "));
        String updateSetClause =
                nonUniqueKeyFields.stream()
                        .map(
                                fieldName ->
                                        String.format(
                                                "[TARGET].%s=[SOURCE].%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(", "));
        String insertFields =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String insertValues =
                Arrays.stream(fieldNames)
                        .map(fieldName -> "[SOURCE]." + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                String.format(
                        "MERGE INTO %s.%s AS [TARGET]"
                                + " USING (%s) AS [SOURCE]"
                                + " ON (%s)"
                                + " WHEN MATCHED THEN"
                                + " UPDATE SET %s"
                                + " WHEN NOT MATCHED THEN"
                                + " INSERT (%s) VALUES (%s);",
                        database,
                        tableName,
                        usingClause,
                        onConditions,
                        updateSetClause,
                        insertFields,
                        insertValues);

        return Optional.of(upsertSQL);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append("[").append(parts[i]).append("]").append(".");
            }
            return sb.append("[")
                    .append(getFieldIde(parts[parts.length - 1], fieldIde))
                    .append("]")
                    .toString();
        }

        return "[" + getFieldIde(identifier, fieldIde) + "]";
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "[" + identifier + "]";
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, true);
    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {

        // 1. If no query is configured, use TABLE STATUS.
        // 2. If a query is configured but does not contain a WHERE clause and tablePath is
        // configured, use TABLE STATUS.
        // 3. If a query is configured with a WHERE clause, or a query statement is configured but
        // tablePath is TablePath.DEFAULT, use COUNT(*).

        boolean useTableStats =
                StringUtils.isBlank(table.getQuery())
                        || (!table.getQuery().toLowerCase().contains("where")
                                && table.getTablePath() != null
                                && !TablePath.DEFAULT
                                        .getFullName()
                                        .equals(table.getTablePath().getFullName()));

        if (useTableStats) {
            TablePath tablePath = table.getTablePath();
            try (Statement stmt = connection.createStatement()) {
                if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
                    String useDatabaseStatement =
                            String.format(
                                    "USE %s;",
                                    quoteDatabaseIdentifier(tablePath.getDatabaseName()));
                    log.info("Split Chunk, approximateRowCntStatement: {}", useDatabaseStatement);
                    stmt.execute(useDatabaseStatement);
                }
                String rowCountQuery =
                        String.format(
                                "SELECT Total_Rows = SUM(st.row_count) FROM sys"
                                        + ".dm_db_partition_stats st WHERE object_name(object_id) = '%s' AND index_id < 2;",
                                tablePath.getTableName());
                log.info("Split Chunk, approximateRowCntStatement: {}", rowCountQuery);
                try (ResultSet rs = stmt.executeQuery(rowCountQuery)) {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        rowCountQuery));
                    }
                    return rs.getLong(1);
                }
            }
        }
        return SQLUtils.countForSubquery(connection, table.getQuery());
    }

    @Override
    public Object queryNextChunkMax(
            Connection connection,
            JdbcSourceTable table,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String quotedColumn = quoteIdentifier(columnName);
        String sqlQuery;
        if (StringUtils.isNotBlank(table.getQuery())) {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT TOP (%s) %s FROM (%s) AS T1 WHERE %s >= ? ORDER BY %s ASC"
                                    + ") AS T2",
                            quotedColumn,
                            chunkSize,
                            quotedColumn,
                            table.getQuery(),
                            quotedColumn,
                            quotedColumn);
        } else {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT TOP (%s) %s FROM %s WHERE %s >= ? ORDER BY %s ASC "
                                    + ") AS T",
                            quotedColumn,
                            chunkSize,
                            quotedColumn,
                            table.getTablePath().getFullName(),
                            quotedColumn,
                            quotedColumn);
        }
        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            ps.setObject(1, includedLowerBound);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getObject(1);
                } else {
                    // this should never happen
                    throw new SQLException(
                            String.format("No result returned after running query [%s]", sqlQuery));
                }
            }
        }
    }
}
