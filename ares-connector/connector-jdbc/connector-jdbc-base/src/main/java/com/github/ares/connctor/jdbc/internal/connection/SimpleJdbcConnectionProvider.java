package com.github.ares.connctor.jdbc.internal.connection;

import com.github.ares.connctor.jdbc.config.JdbcConnectionConfig;
import com.github.ares.connctor.jdbc.exception.JdbcConnectorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

import static com.github.ares.com.google.common.base.Preconditions.checkNotNull;

/** Simple JDBC connection provider. */
public class SimpleJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);

    private static final long serialVersionUID = -1L;

    protected final JdbcConnectionConfig jdbcConfig;

    private transient Driver loadedDriver;
    protected transient Connection connection;

    public SimpleJdbcConnectionProvider(JdbcConnectionConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return connection != null
                && connection.isValid(jdbcConfig.getConnectionCheckTimeoutSeconds());
    }

    private static Driver loadDriver(String driverName) throws ClassNotFoundException {
        checkNotNull(driverName);
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverName)) {
                return driver;
            }
        }

        // We could reach here for reasons:
        // * Class loader hell of DriverManager(see JDK-8146872).
        // * driver is not installed as a service provider.
        Class<?> clazz =
                Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
        try {
            return (Driver) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new JdbcConnectorException(
                    "Fail to create driver of class " + driverName,
                    ex);
        }
    }

    protected Driver getLoadedDriver() throws SQLException, ClassNotFoundException {
        if (loadedDriver == null) {
            loadedDriver = loadDriver(jdbcConfig.getDriverName());
        }
        return loadedDriver;
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (isConnectionValid()) {
            return connection;
        }
        Driver driver = getLoadedDriver();
        Properties info = new Properties();
        if (jdbcConfig.getUsername().isPresent()) {
            info.setProperty("user", jdbcConfig.getUsername().get());
        }
        if (jdbcConfig.getPassword().isPresent()) {
            info.setProperty("password", jdbcConfig.getPassword().get());
        }
        info.putAll(jdbcConfig.getProperties());
        connection = driver.connect(jdbcConfig.getUrl(), info);
        if (connection == null) {
            // Throw same exception as DriverManager.getConnection when no driver found to match
            // caller expectation.
            throw new JdbcConnectorException(
                    "No suitable driver found for " + jdbcConfig.getUrl());
        }

        connection.setAutoCommit(jdbcConfig.isAutoCommit());

        return connection;
    }

    @Override
    public void closeConnection() {
        try {
            if (isConnectionValid()) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.warn("JDBC connection close failed.", e);
        } finally {
            connection = null;
        }
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        closeConnection();
        return getOrEstablishConnection();
    }

    public JdbcConnectionConfig getJdbcConfig() {
        return jdbcConfig;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}
