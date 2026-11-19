package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.connector.jdbc.internal.converter.JdbcRowConverter;
import com.github.ares.connector.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import com.github.ares.connector.jdbc.source.JdbcSourceTable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.lang3.StringUtils;

public class Gbase8sDialect implements JdbcDialect {

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public Gbase8sDialect() {}

    public Gbase8sDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.GBASE_8S;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new Gbase8sJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new Gbase8sTypeMapper();
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return "MOD(ABS(" + quoteIdentifier(fieldName) + ")," + mod + ")";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) {
                    sb.append('.');
                }
                String part = parts[i];
                if (i == parts.length - 1) {
                    part = getFieldIde(part, fieldIde);
                }
                sb.append('"').append(part.replace("\"", "\"\"")).append('"');
            }
            return sb.toString();
        }
        return "\"" + getFieldIde(identifier, fieldIde).replace("\"", "\"\"") + "\"";
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, true);
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
        String source =
                StringUtils.isNotBlank(table.getQuery())
                        ? "(" + table.getQuery() + ") AS T1"
                        : tableIdentifier(table.getTablePath());
        String sqlQuery = nextChunkSql(quotedColumn, source, chunkSize);
        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            ps.setObject(1, includedLowerBound);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getObject(1);
                }
                throw new SQLException(
                        String.format("No result returned after running query [%s]", sqlQuery));
            }
        }
    }

    static String nextChunkSql(String quotedColumn, String source, int chunkSize) {
        return String.format(
                "SELECT MAX(%s) FROM ("
                        + "SELECT FIRST %s %s FROM %s WHERE %s >= ? ORDER BY %s ASC) AS T",
                quotedColumn, chunkSize, quotedColumn, source, quotedColumn, quotedColumn);
    }
}
