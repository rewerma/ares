package com.github.ares.connctor.jdbc.internal.dialect.psql;

import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectFactory;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class PostgresDialectFactory implements JdbcDialectFactory {
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
        /*if ("postgresLow".equalsIgnoreCase(compatibleMode)) {
            return new PostgresLowDialect(fieldIde);
        }*/
        return new PostgresDialect(fieldIde);
    }
}
