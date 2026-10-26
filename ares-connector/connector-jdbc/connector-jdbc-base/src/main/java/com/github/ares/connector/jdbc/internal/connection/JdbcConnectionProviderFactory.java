package com.github.ares.connector.jdbc.internal.connection;

import com.github.ares.connector.jdbc.config.JdbcConnectionConfig;

public final class JdbcConnectionProviderFactory {

    private JdbcConnectionProviderFactory() {}

    public static JdbcConnectionProvider create(JdbcConnectionConfig jdbcConnectionConfig) {
        if (jdbcConnectionConfig.isConnectionPoolEnabled()) {
            return new PooledJdbcConnectionProvider(jdbcConnectionConfig);
        }
        return new SimpleJdbcConnectionProvider(jdbcConnectionConfig);
    }
}
