package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.connector.jdbc.internal.converter.JdbcRowConverter;
import com.github.ares.connector.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

public class ClickHouseDialect implements JdbcDialect {

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public ClickHouseDialect() {}

    public ClickHouseDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.CLICKHOUSE;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new ClickHouseJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new ClickHouseTypeMapper();
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return "modulo(cityHash64(" + quoteIdentifier(fieldName) + ")," + mod + ")";
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
                sb.append(quoteSingle(part));
            }
            return sb.toString();
        }
        return quoteSingle(getFieldIde(identifier, fieldIde));
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return quoteSingle(identifier);
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, false);
    }

    private static String quoteSingle(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
