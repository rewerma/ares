package com.github.ares.connctor.jdbc.internal.dialect;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

/** Factory for {@link SqlServerDialect}. */
@AutoService(JdbcDialectFactory.class)
public class SqlServerDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectIdentifier() {
        return DatabaseIdentifier.SQLSERVER;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:sqlserver:");
    }

    @Override
    public JdbcDialect create() {
        return new SqlServerDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new SqlServerDialect(fieldIde);
    }
}
