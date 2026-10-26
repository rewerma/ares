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
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Doris speaks the MySQL protocol on the FE query port. */
public class DorisDialect implements JdbcDialect {
    private static final Logger log = LoggerFactory.getLogger(DorisDialect.class);

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public DorisDialect() {}

    public DorisDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.DORIS;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new DorisJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new DorisTypeMapper();
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return "MOD(ABS(murmur_hash3_32(" + quoteIdentifier(fieldName) + ")), " + mod + ")";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append(quoteSingle(parts[i])).append(".");
            }
            return sb.append(quoteSingle(getFieldIde(parts[parts.length - 1], fieldIde)))
                    .toString();
        }
        return quoteSingle(getFieldIde(identifier, fieldIde));
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return quoteSingle(identifier);
    }

    private static String quoteSingle(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(Integer.MIN_VALUE);
        return statement;
    }

    @Override
    public String extractTableName(TablePath tablePath) {
        return tablePath.getTableName();
    }

    @Override
    public Map<String, String> defaultParameter() {
        HashMap<String, String> map = new HashMap<>();
        map.put("rewriteBatchedStatements", "true");
        map.put("tinyInt1isBit", "false");
        map.put("useUnicode", "true");
        map.put("characterEncoding", "UTF-8");
        map.put("zeroDateTimeBehavior", "CONVERT_TO_NULL");
        return map;
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, false);
    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {
        if (StringUtils.isNotBlank(table.getQuery())
                && table.getQuery().toLowerCase().contains("where")) {
            return SQLUtils.countForSubquery(connection, table.getQuery());
        }
        TablePath tablePath = table.getTablePath();
        if (tablePath != null
                && StringUtils.isNotBlank(tablePath.getDatabaseName())
                && StringUtils.isNotBlank(tablePath.getTableName())) {
            String rowCountQuery =
                    String.format(
                            "SELECT TABLE_ROWS FROM information_schema.tables "
                                    + "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                            tablePath.getDatabaseName().replace("'", "''"),
                            tablePath.getTableName().replace("'", "''"));
            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(rowCountQuery)) {
                if (rs.next()) {
                    long rows = rs.getLong(1);
                    if (!rs.wasNull()) {
                        log.info("Split Chunk, approximateRowCntStatement: {}", rowCountQuery);
                        return rows;
                    }
                }
            } catch (SQLException e) {
                log.warn("Failed to read information_schema.tables row count: {}", e.getMessage());
            }
        }
        if (StringUtils.isNotBlank(table.getQuery())) {
            return SQLUtils.countForSubquery(connection, table.getQuery());
        }
        return SQLUtils.countForTable(connection, tableIdentifier(table.getTablePath()));
    }
}
