package com.github.ares.connector.jdbc.internal.connection;

import com.github.ares.connector.jdbc.config.JdbcConnectionConfig;
import com.github.ares.connector.jdbc.exception.JdbcConnectorException;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

/** JDBC connection provider backed by a shared HikariCP pool. */
public class PooledJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(PooledJdbcConnectionProvider.class);

    private static final long serialVersionUID = 1L;

    private final JdbcConnectionConfig jdbcConfig;

    private transient Connection connection;

    public PooledJdbcConnectionProvider(JdbcConnectionConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return connection != null
                && !connection.isClosed()
                && connection.isValid(jdbcConfig.getConnectionCheckTimeoutSeconds());
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (isConnectionValid()) {
            return connection;
        }
        try {
            connection = getDataSource().getConnection();
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    "Failed to borrow JDBC connection from pool for " + jdbcConfig.getUrl(), e);
        }
        if (connection.getAutoCommit() != jdbcConfig.isAutoCommit()) {
            connection.setAutoCommit(jdbcConfig.isAutoCommit());
        }
        jdbcConfig.applySessionSettings(connection);
        return connection;
    }

    @Override
    public void closeConnection() {
        if (connection == null) {
            return;
        }
        try {
            if (!connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.warn("JDBC pooled connection close failed.", e);
        } finally {
            connection = null;
        }
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        closeConnection();
        return getOrEstablishConnection();
    }

    private HikariDataSource getDataSource() {
        return JdbcDataSourcePool.getDataSource(jdbcConfig);
    }

    public JdbcConnectionConfig getJdbcConfig() {
        return jdbcConfig;
    }
}
