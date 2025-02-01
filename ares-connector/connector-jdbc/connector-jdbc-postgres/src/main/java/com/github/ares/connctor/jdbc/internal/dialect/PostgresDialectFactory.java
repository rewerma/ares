package com.github.ares.connctor.jdbc.internal.dialect;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class PostgresDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectIdentifier() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:postgresql:");
    }

    @Override
    public JdbcDialect create() {
        throw new UnsupportedOperationException(
                "Can't create JdbcDialect without compatible mode for Postgres");
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new PostgresDialect(fieldIde);
    }
}
