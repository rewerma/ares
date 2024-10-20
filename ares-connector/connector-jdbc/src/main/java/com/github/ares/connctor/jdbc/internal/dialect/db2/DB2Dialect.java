package com.github.ares.connctor.jdbc.internal.dialect.db2;

import com.github.ares.connctor.jdbc.internal.converter.JdbcRowConverter;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.util.Optional;

public class DB2Dialect implements JdbcDialect {

    @Override
    public String dialectName() {
        return DatabaseIdentifier.DB_2;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new DB2JdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new DB2TypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }
}
