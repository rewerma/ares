package com.github.ares.connector.jdbc.internal.dialect;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

/** Factory for {@link MysqlDialect}. */
@AutoService(JdbcDialectFactory.class)
public class MySqlDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectIdentifier() {
        return DatabaseIdentifier.MYSQL;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:mysql:");
    }

    @Override
    public JdbcDialect create() {
        return new MysqlDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new MysqlDialect(fieldIde);
    }
}
