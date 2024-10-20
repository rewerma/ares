package com.github.ares.connctor.jdbc.internal.connection;

import com.github.ares.connctor.jdbc.config.JdbcConnectionConfig;
import com.github.ares.connctor.jdbc.sink.ConnectionPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class SimpleJdbcConnectionPoolProviderProxy implements JdbcConnectionProvider {
    private static final Logger log = LoggerFactory.getLogger(SimpleJdbcConnectionPoolProviderProxy.class);

    private final transient ConnectionPoolManager poolManager;
    private final JdbcConnectionConfig jdbcConfig;
    private final int queueIndex;

    public SimpleJdbcConnectionPoolProviderProxy(
            ConnectionPoolManager poolManager, JdbcConnectionConfig jdbcConfig, int queueIndex) {
        this.jdbcConfig = jdbcConfig;
        this.poolManager = poolManager;
        this.queueIndex = queueIndex;
    }

    @Override
    public Connection getConnection() {
        return poolManager.getConnection(queueIndex);
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return poolManager.containsConnection(queueIndex)
                && poolManager
                .getConnection(queueIndex)
                .isValid(jdbcConfig.getConnectionCheckTimeoutSeconds());
    }

    @Override
    public Connection getOrEstablishConnection() {
        return poolManager.getConnection(queueIndex);
    }

    @Override
    public void closeConnection() {
        if (poolManager.containsConnection(queueIndex)) {
            try {
                poolManager.remove(queueIndex).close();
            } catch (SQLException e) {
                log.warn("JDBC connection close failed.", e);
            }
        }
    }

    @Override
    public Connection reestablishConnection() {
        closeConnection();
        return getOrEstablishConnection();
    }
}
