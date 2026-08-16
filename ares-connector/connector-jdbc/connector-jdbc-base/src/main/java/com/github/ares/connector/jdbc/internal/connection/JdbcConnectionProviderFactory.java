package com.github.ares.connector.jdbc.internal.connection;

import com.github.ares.connector.jdbc.config.JdbcConnectionConfig;
import org.apache.commons.lang3.StringUtils;

public final class JdbcConnectionProviderFactory {

    private JdbcConnectionProviderFactory() {}

    public static JdbcConnectionProvider create(JdbcConnectionConfig jdbcConnectionConfig) {
        if (jdbcConnectionConfig.isConnectionPoolEnabled()
                && StringUtils.isBlank(jdbcConnectionConfig.getXaDataSourceClassName())) {
            return new PooledJdbcConnectionProvider(jdbcConnectionConfig);
        }
        return new SimpleJdbcConnectionProvider(jdbcConnectionConfig);
    }
}
