package com.github.ares.connector.jdbc.internal.dialect;

import com.google.auto.service.AutoService;
import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class ClickHouseDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectIdentifier() {
        return DatabaseIdentifier.CLICKHOUSE;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:clickhouse:") || url.startsWith("jdbc:ch:");
    }

    @Override
    public JdbcDialect create() {
        return new ClickHouseDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new ClickHouseDialect(fieldIde);
    }
}
