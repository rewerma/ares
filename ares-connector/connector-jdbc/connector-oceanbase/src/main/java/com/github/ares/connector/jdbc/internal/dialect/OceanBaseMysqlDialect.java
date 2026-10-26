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

public class OceanBaseMysqlDialect implements JdbcDialect {
    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public OceanBaseMysqlDialect() {}

    public OceanBaseMysqlDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.OCEANBASE;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new OceanBaseJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new OceanBaseMysqlTypeMapper();
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return "MOD(ABS(CRC32(" + quoteIdentifier(fieldName) + ")), " + mod + ")";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append("`").append(parts[i]).append("`.");
            }
            return sb.append("`")
                    .append(getFieldIde(parts[parts.length - 1], fieldIde))
                    .append("`")
                    .toString();
        }
        return "`" + getFieldIde(identifier, fieldIde) + "`";
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "`" + identifier + "`";
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
            statement.setFetchSize(Integer.MIN_VALUE);
        }
        return statement;
    }

    @Override
    public String extractTableName(TablePath tablePath) {
        return tablePath.getTableName();
    }

    @Override
    public Map<String, String> defaultParameter() {
        Map<String, String> map = new HashMap<>();
        map.put("rewriteBatchedStatements", "true");
        return map;
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, false);
    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {
        if (StringUtils.isBlank(table.getQuery())) {
            TablePath tablePath = table.getTablePath();
            String useDatabaseStatement = null;
            if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
                useDatabaseStatement =
                        String.format(
                                "USE %s", quoteDatabaseIdentifier(tablePath.getDatabaseName()));
            }
            String rowCountQuery =
                    String.format("SHOW TABLE STATUS LIKE '%s'", tablePath.getTableName());
            try (Statement stmt = connection.createStatement()) {
                if (useDatabaseStatement != null) {
                    stmt.execute(useDatabaseStatement);
                }
                try (ResultSet rs = stmt.executeQuery(rowCountQuery)) {
                    if (!rs.next() || rs.getMetaData().getColumnCount() < 5) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        rowCountQuery));
                    }
                    return rs.getLong(5);
                }
            }
        }
        return SQLUtils.countForSubquery(connection, table.getQuery());
    }
}
