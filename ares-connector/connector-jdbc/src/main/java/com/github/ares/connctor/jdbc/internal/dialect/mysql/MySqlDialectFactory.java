package com.github.ares.connctor.jdbc.internal.dialect.mysql;

import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectFactory;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

/** Factory for {@link MysqlDialect}. */
@AutoService(JdbcDialectFactory.class)
public class MySqlDialectFactory implements JdbcDialectFactory {
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
