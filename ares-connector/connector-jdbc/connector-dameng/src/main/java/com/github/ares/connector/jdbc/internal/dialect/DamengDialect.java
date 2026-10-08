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

public class DamengDialect implements JdbcDialect {
    private static final Logger log = LoggerFactory.getLogger(DamengDialect.class);

    private static final int DEFAULT_DAMENG_FETCH_SIZE = 128;
    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public DamengDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    public DamengDialect() {}

    @Override
    public String dialectName() {
        return DatabaseIdentifier.DAMENG;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new DamengJdbcRowConverter();
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return "MOD(ORA_HASH(" + quoteIdentifier(fieldName) + ")," + mod + ")";
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new DamengTypeMapper();
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
    public String tableIdentifier(String database, String tableName) {
        return quoteIdentifier(tableName);
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        } else {
            statement.setFetchSize(DEFAULT_DAMENG_FETCH_SIZE);
        }
        return statement;
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, true);
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {
        boolean useTableStats =
                StringUtils.isBlank(table.getQuery())
                        || (!table.getQuery().toLowerCase().contains("where")
                                && table.getTablePath() != null);
        if (useTableStats) {
            TablePath tablePath = table.getTablePath();
            String analyzeTable =
                    String.format(
                            "analyze table %s compute statistics for table",
                            tablePath.getSchemaAndTableName());
            String rowCountQuery =
                    String.format(
                            "select NUM_ROWS from all_tables where OWNER = '%s' AND TABLE_NAME = '%s' ",
                            tablePath.getSchemaName(), tablePath.getTableName());

            try (Statement stmt = connection.createStatement()) {
                log.info("Split Chunk, approximateRowCntStatement: {}", analyzeTable);
                stmt.execute(analyzeTable);
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
                                    + "SELECT %s FROM (%s) WHERE %s >= ? ORDER BY %s ASC "
                                    + ") WHERE ROWNUM <= %s",
                            quotedColumn,
                            quotedColumn,
                            table.getQuery(),
                            quotedColumn,
                            quotedColumn,
                            chunkSize);
        } else {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT %s FROM %s WHERE %s >= ? ORDER BY %s ASC "
                                    + ") WHERE ROWNUM <= %s",
                            quotedColumn,
                            quotedColumn,
                            table.getTablePath().getSchemaAndTableName(),
                            quotedColumn,
                            quotedColumn,
                            chunkSize);
        }

        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            ps.setObject(1, includedLowerBound);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException(
                            String.format("No result returned after running query [%s]", sqlQuery));
                }
                return rs.getObject(1);
            }
        }
    }
}
