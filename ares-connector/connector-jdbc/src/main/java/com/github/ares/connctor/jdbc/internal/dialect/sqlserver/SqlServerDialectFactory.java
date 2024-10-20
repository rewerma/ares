package com.github.ares.connctor.jdbc.internal.dialect.sqlserver;

import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectFactory;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

/** Factory for {@link SqlServerDialect}. */
@AutoService(JdbcDialectFactory.class)
public class SqlServerDialectFactory implements JdbcDialectFactory {
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
