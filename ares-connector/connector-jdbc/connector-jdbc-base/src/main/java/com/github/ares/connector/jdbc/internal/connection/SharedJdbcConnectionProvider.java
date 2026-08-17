package com.github.ares.connector.jdbc.internal.connection;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Connection provider that reuses a Driver-held JDBC connection and does not close it.
 */
public class SharedJdbcConnectionProvider implements JdbcConnectionProvider {

    private final JdbcConnectionProvider delegate;

    public SharedJdbcConnectionProvider(JdbcConnectionProvider delegate) {
        this.delegate = delegate;
    }

    @Override
    public Connection getConnection() {
        return delegate.getConnection();
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return delegate.isConnectionValid();
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        return delegate.getOrEstablishConnection();
    }

    @Override
    public void closeConnection() {
        // owned by the PL transaction session
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        return delegate.reestablishConnection();
    }
}
