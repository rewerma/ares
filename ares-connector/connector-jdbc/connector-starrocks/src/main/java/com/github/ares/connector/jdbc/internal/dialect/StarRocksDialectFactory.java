package com.github.ares.connector.jdbc.internal.dialect;

import com.google.auto.service.AutoService;
import javax.annotation.Nonnull;

/** Factory for {@link StarRocksDialect}. */
@AutoService(JdbcDialectFactory.class)
public class StarRocksDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectIdentifier() {
        return DatabaseIdentifier.STARROCKS;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:mysql:");
    }

    @Override
    public JdbcDialect create() {
        return new StarRocksDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new StarRocksDialect(fieldIde);
    }
}
