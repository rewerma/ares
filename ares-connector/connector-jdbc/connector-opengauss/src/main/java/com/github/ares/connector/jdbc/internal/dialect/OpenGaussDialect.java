package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.connector.jdbc.internal.converter.JdbcRowConverter;
import com.github.ares.connector.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import com.github.ares.connector.jdbc.source.JdbcSourceTable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenGaussDialect implements JdbcDialect {
    private static final Logger log = LoggerFactory.getLogger(OpenGaussDialect.class);

    public static final int DEFAULT_OPENGAUSS_FETCH_SIZE = 128;

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public OpenGaussDialect() {}

    public OpenGaussDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.OPENGAUSS;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new OpenGaussJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new OpenGaussTypeMapper();
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return "(ABS(HASHTEXT(" + quoteIdentifier(fieldName) + ")) % " + mod + ")";
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        // OpenGauss follows the PostgreSQL cursor protocol.
        connection.setAutoCommit(false);
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        } else {
            statement.setFetchSize(DEFAULT_OPENGAUSS_FETCH_SIZE);
        }
        return statement;
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        return quoteDatabaseIdentifier(database) + "." + quoteIdentifier(tableName);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append("\"").append(parts[i]).append("\".");
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
        boolean useTableStats =
                StringUtils.isBlank(table.getQuery())
                        || (!table.getQuery().toLowerCase().contains("where")
                                && table.getTablePath() != null);
        if (useTableStats) {
            String rowCountQuery = rowCountQuery(table.getTablePath());
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

    private static String rowCountQuery(TablePath tablePath) {
        if (StringUtils.isNotBlank(tablePath.getSchemaName())) {
            return String.format(
                    "SELECT r.reltuples FROM pg_class r "
                            + "JOIN pg_namespace n ON r.relnamespace = n.oid "
                            + "WHERE r.relkind = 'r' AND n.nspname = '%s' AND r.relname = '%s'",
                    tablePath.getSchemaName(), tablePath.getTableName());
        }
        return String.format(
                "SELECT reltuples FROM pg_class r WHERE relkind = 'r' AND relname = '%s'",
                tablePath.getTableName());
    }
}
