package com.github.ares.connector.jdbc.internal.connection;

import com.github.ares.connector.jdbc.config.JdbcConnectionConfig;
import com.github.ares.connector.jdbc.exception.JdbcConnectorException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared HikariCP pools keyed by JDBC connection configuration. */
final class JdbcDataSourcePool {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDataSourcePool.class);

    private static final ConcurrentHashMap<String, HikariDataSource> POOLS =
            new ConcurrentHashMap<>();

    private JdbcDataSourcePool() {}

    static HikariDataSource getDataSource(JdbcConnectionConfig jdbcConfig) {
        return POOLS.computeIfAbsent(
                buildPoolKey(jdbcConfig), key -> createDataSource(jdbcConfig, key));
    }

    private static HikariDataSource createDataSource(
            JdbcConnectionConfig jdbcConfig, String poolKey) {
        loadDriverClass(jdbcConfig.getDriverName());
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setPoolName("ares-jdbc-" + poolKey);
        hikariConfig.setJdbcUrl(jdbcConfig.getUrl());
        hikariConfig.setDriverClassName(jdbcConfig.getDriverName());
        jdbcConfig.getUsername().ifPresent(hikariConfig::setUsername);
        jdbcConfig.getPassword().ifPresent(hikariConfig::setPassword);
        hikariConfig.setMaximumPoolSize(jdbcConfig.getPoolSize());
        hikariConfig.setMinimumIdle(Math.min(1, jdbcConfig.getPoolSize()));
        hikariConfig.setConnectionTimeout(jdbcConfig.getConnectionCheckTimeoutSeconds() * 1000L);
        hikariConfig.setAutoCommit(jdbcConfig.isAutoCommit());
        for (Map.Entry<String, String> entry : jdbcConfig.getProperties().entrySet()) {
            hikariConfig.addDataSourceProperty(entry.getKey(), entry.getValue());
        }
        LOG.info(
                "Create JDBC connection pool {} for url {}, maxPoolSize={}",
                hikariConfig.getPoolName(),
                jdbcConfig.getUrl(),
                jdbcConfig.getPoolSize());
        return new HikariDataSource(hikariConfig);
    }

    private static String buildPoolKey(JdbcConnectionConfig jdbcConfig) {
        String propertiesKey =
                jdbcConfig.getProperties().entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                        .collect(Collectors.joining("&"));
        return Integer.toHexString(
                Objects.hash(
                        jdbcConfig.getUrl(),
                        jdbcConfig.getDriverName(),
                        jdbcConfig.getUsername(),
                        jdbcConfig.getPassword(),
                        jdbcConfig.isAutoCommit(),
                        jdbcConfig.getPoolSize(),
                        propertiesKey));
    }

    private static void loadDriverClass(String driverName) {
        try {
            Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new JdbcConnectorException("Failed to load JDBC driver class " + driverName, e);
        }
    }
}
