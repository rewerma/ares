package com.github.ares.connector.jdbc.internal.dialect;

import com.google.auto.service.AutoService;
import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class Db2DialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectIdentifier() {
        return DatabaseIdentifier.DB_2;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:db2:");
    }

    @Override
    public JdbcDialect create() {
        return new Db2Dialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new Db2Dialect(fieldIde);
    }
}
