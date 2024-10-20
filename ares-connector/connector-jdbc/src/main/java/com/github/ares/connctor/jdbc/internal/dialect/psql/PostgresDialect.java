package com.github.ares.connctor.jdbc.internal.dialect.psql;

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
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class PostgresDialect implements JdbcDialect {

    private static final long serialVersionUID = -5834746193472465218L;
    public static final int DEFAULT_POSTGRES_FETCH_SIZE = 128;

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public PostgresDialect() {}

    public PostgresDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new PostgresJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new PostgresTypeMapper();
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return "(ABS(HASHTEXT(" + quoteIdentifier(fieldName) + ")) % " + mod + ")";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String uniqueColumns =
                Arrays.stream(uniqueKeyFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String updateClause =
                Arrays.stream(fieldNames)
                        .map(
                                fieldName ->
                                        quoteIdentifier(fieldName)
                                                + "=EXCLUDED."
                                                + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                String.format(
                        "%s ON CONFLICT (%s) DO UPDATE SET %s",
                        getInsertIntoStatement(database, tableName, fieldNames),
                        uniqueColumns,
                        updateClause);
        return Optional.of(upsertSQL);
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        // use cursor mode, reference:
        // https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
        connection.setAutoCommit(false);
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        } else {
            statement.setFetchSize(DEFAULT_POSTGRES_FETCH_SIZE);
        }
        return statement;
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        // resolve pg database name upper or lower not recognised
        return quoteDatabaseIdentifier(database) + "." + quoteIdentifier(tableName);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append("\"").append(parts[i]).append("\"").append(".");
            }
            return sb.append("\"")
                    .append(getFieldIde(parts[parts.length - 1], fieldIde))
                    .append("\"")
                    .toString();
        }

        return "\"" + getFieldIde(identifier, fieldIde) + "\"";
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "\"" + identifier + "\"";
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
        // tablePath is not, use COUNT(*).

        boolean useTableStats =
                StringUtils.isBlank(table.getQuery())
                        || (!table.getQuery().toLowerCase().contains("where")
                                && table.getTablePath() != null);
        if (useTableStats) {
            String rowCountQuery =
                    String.format(
                            "SELECT reltuples FROM pg_class r WHERE relkind = 'r' AND relname = '%s';",
                            table.getTablePath().getTableName());
            try (Statement stmt = connection.createStatement()) {
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
}
