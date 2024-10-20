package com.github.ares.connctor.jdbc.internal.dialect.mysql;

import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.connctor.jdbc.catalog.AbstractJdbcCatalog;
import com.github.ares.connctor.jdbc.internal.converter.JdbcRowConverter;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectTypeMapper;
import com.github.ares.connctor.jdbc.internal.dialect.SQLUtils;
import com.github.ares.connctor.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import com.github.ares.connctor.jdbc.source.JdbcSourceTable;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MysqlDialect implements JdbcDialect {
    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public MysqlDialect() {
    }

    public MysqlDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.MYSQL;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new MysqlJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new MySqlTypeMapper();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + getFieldIde(identifier, fieldIde) + "`";
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String updateClause =
                Arrays.stream(fieldNames)
                        .map(
                                fieldName ->
                                        quoteIdentifier(fieldName)
                                                + "=VALUES("
                                                + quoteIdentifier(fieldName)
                                                + ")")
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                getInsertIntoStatement(database, tableName, fieldNames)
                        + " ON DUPLICATE KEY UPDATE "
                        + updateClause;
        return Optional.of(upsertSQL);
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
        return map;
    }

    @Override
    public TablePath parse(AbstractJdbcCatalog jdbcCatalog, String tablePath) {
        TablePath tp = parse(tablePath);
        if (tp.getDatabaseName() == null && jdbcCatalog.getDefaultDatabase() != null) {
            return TablePath.of(jdbcCatalog.getDefaultDatabase(), null, tp.getTableName());
        }
        return tp;
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, false);
    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {
        if (StringUtils.isBlank(table.getQuery())) {
            // The statement used to get approximate row count which is less
            // accurate than COUNT(*), but is more efficient for large table.
            TablePath tablePath = table.getTablePath();
            String useDatabaseStatement = null;
            if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
                useDatabaseStatement = String.format("USE %s;", quoteDatabaseIdentifier(tablePath.getDatabaseName()));
            }
            String rowCountQuery =
                    String.format("SHOW TABLE STATUS LIKE '%s';", tablePath.getTableName());
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
