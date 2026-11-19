package com.github.ares.connector.jdbc.internal.dialect;

import com.google.auto.service.AutoService;
import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class VerticaDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectIdentifier() {
        return DatabaseIdentifier.VERTICA;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:vertica:");
    }

    @Override
    public JdbcDialect create() {
        return new VerticaDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new VerticaDialect(fieldIde);
    }
}
