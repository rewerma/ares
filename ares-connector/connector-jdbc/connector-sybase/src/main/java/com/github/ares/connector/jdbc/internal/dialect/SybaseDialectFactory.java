package com.github.ares.connector.jdbc.internal.dialect;

import com.google.auto.service.AutoService;
import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class SybaseDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectIdentifier() {
        return DatabaseIdentifier.SYBASE;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:jtds:sybase:") || url.startsWith("jdbc:sybase:");
    }

    @Override
    public JdbcDialect create() {
        return new SybaseDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new SybaseDialect(fieldIde);
    }
}
