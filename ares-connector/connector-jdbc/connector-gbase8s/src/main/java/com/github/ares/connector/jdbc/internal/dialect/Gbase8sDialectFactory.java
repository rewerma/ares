package com.github.ares.connector.jdbc.internal.dialect;

import com.google.auto.service.AutoService;
import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class Gbase8sDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectIdentifier() {
        return DatabaseIdentifier.GBASE_8S;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:gbasedbt-sqli:");
    }

    @Override
    public JdbcDialect create() {
        return new Gbase8sDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new Gbase8sDialect(fieldIde);
    }
}
