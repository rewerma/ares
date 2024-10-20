package com.github.ares.connctor.jdbc.internal.dialect.hive;

import com.github.ares.connctor.jdbc.config.JdbcConnectionConfig;
import com.github.ares.connctor.jdbc.exception.JdbcConnectorException;
import com.github.ares.connctor.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import lombok.NonNull;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public class HiveJdbcConnectionProvider extends SimpleJdbcConnectionProvider {

    public HiveJdbcConnectionProvider(@NonNull JdbcConnectionConfig jdbcConfig) {
        super(jdbcConfig);
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (isConnectionValid()) {
            return super.getConnection();
        }
        JdbcConnectionConfig jdbcConfig = super.getJdbcConfig();
        final Driver driver = getLoadedDriver();
        HiveConnectionProduceFunction hiveConnectionProduceFunction =
                new HiveConnectionProduceFunction(driver, jdbcConfig);

        if (jdbcConfig.useKerberos) {
            super.setConnection(getConnectionWithKerberos(hiveConnectionProduceFunction));
        } else {
            super.setConnection(hiveConnectionProduceFunction.produce());
        }
        if (super.getConnection() == null) {
            // Throw same exception as DriverManager.getConnection when no driver found to match
            // caller expectation.
            throw new JdbcConnectorException(
                    "No suitable driver found for " + super.getJdbcConfig().getUrl());
        }
        return super.getConnection();
    }

    private Connection getConnectionWithKerberos(
            HiveConnectionProduceFunction hiveConnectionProduceFunction) {
        try {
            Configuration configuration = new Configuration();
            configuration.set("hadoop.security.authentication", "kerberos");
            return HadoopLoginFactory.loginWithKerberos(
                    configuration,
                    jdbcConfig.krb5Path,
                    jdbcConfig.kerberosPrincipal,
                    jdbcConfig.kerberosKeytabPath,
                    (conf, userGroupInformation) -> hiveConnectionProduceFunction.produce());
        } catch (Exception ex) {
            throw new JdbcConnectorException("Kerberos authentication failed", ex);
        }
    }

    public static class HiveConnectionProduceFunction {

        private final Driver driver;
        private final JdbcConnectionConfig jdbcConnectionConfig;

        public HiveConnectionProduceFunction(
                Driver driver, JdbcConnectionConfig jdbcConnectionConfig) {
            this.driver = driver;
            this.jdbcConnectionConfig = jdbcConnectionConfig;
        }

        public Connection produce() throws SQLException {
            final Properties info = new Properties();
            jdbcConnectionConfig
                    .getUsername()
                    .ifPresent(username -> info.setProperty("user", username));
            jdbcConnectionConfig
                    .getPassword()
                    .ifPresent(username -> info.setProperty("password", username));
            return driver.connect(jdbcConnectionConfig.getUrl(), info);
        }
    }
}
