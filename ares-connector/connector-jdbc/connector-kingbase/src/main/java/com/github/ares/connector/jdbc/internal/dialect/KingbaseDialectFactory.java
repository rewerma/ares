package com.github.ares.connector.jdbc.internal.dialect;

import com.google.auto.service.AutoService;
import javax.annotation.Nonnull;

/** Factory for {@link KingbaseDialect}. */
@AutoService(JdbcDialectFactory.class)
public class KingbaseDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectIdentifier() {
        return DatabaseIdentifier.KINGBASE;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:kingbase8:") || url.startsWith("jdbc:kingbase:");
    }

    @Override
    public JdbcDialect create() {
        return new KingbaseDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new KingbaseDialect(fieldIde);
    }
}
