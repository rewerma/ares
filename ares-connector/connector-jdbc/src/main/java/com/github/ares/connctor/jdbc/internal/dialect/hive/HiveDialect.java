package com.github.ares.connctor.jdbc.internal.dialect.hive;

import com.github.ares.connctor.jdbc.config.JdbcConnectionConfig;
import com.github.ares.connctor.jdbc.internal.connection.JdbcConnectionProvider;
import com.github.ares.connctor.jdbc.internal.converter.JdbcRowConverter;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Optional;

public class HiveDialect implements JdbcDialect {

    @Override
    public String dialectName() {
        return DatabaseIdentifier.HIVE;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new HiveJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new HiveTypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    @Override
    public ResultSetMetaData getResultSetMetaData(Connection conn, String query)
            throws SQLException {
        return conn.prepareStatement(query).executeQuery().getMetaData();
    }

    @Override
    public JdbcConnectionProvider getJdbcConnectionProvider(
            JdbcConnectionConfig jdbcConnectionConfig) {
        return new HiveJdbcConnectionProvider(jdbcConnectionConfig);
    }
}
