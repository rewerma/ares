package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.connector.jdbc.internal.converter.JdbcRowConverter;
import com.github.ares.connector.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

public class VerticaDialect implements JdbcDialect {

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public VerticaDialect() {}

    public VerticaDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.VERTICA;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new VerticaJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new VerticaTypeMapper();
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return "MOD(ABS(HASH(" + quoteIdentifier(fieldName) + "))," + mod + ")";
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
}
