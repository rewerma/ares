package com.github.ares.connector.jdbc.config;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.common.configuration.ReadonlyConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JdbcConnectionConfig implements Serializable {
    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionConfig.class);

    public String dbType;
    public String url;
    public String driverName;
    public String compatibleMode;
    public int connectionCheckTimeoutSeconds =
            JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC.defaultValue();
    public boolean connectionPoolEnabled = JdbcOptions.CONNECTION_POOL_ENABLED.defaultValue();
    public int poolSize = JdbcOptions.POOL_SIZE.defaultValue();
    public int queryTimeoutSec = JdbcOptions.QUERY_TIMEOUT_SEC.defaultValue();
    public int maxRetries = JdbcOptions.MAX_RETRIES.defaultValue();
    public String username;
    public String password;
    public String query;

    public boolean autoCommit = JdbcOptions.AUTO_COMMIT.defaultValue();

    public int batchSize = JdbcOptions.BATCH_SIZE.defaultValue();

    private Map<String, String> properties;

    public static JdbcConnectionConfig of(ReadonlyConfig config) {
        Builder builder = JdbcConnectionConfig.builder();
        builder.dbType(config.get(CommonOptions.CONNECTOR));
        builder.url(config.get(JdbcOptions.URL));
        builder.compatibleMode(config.get(JdbcOptions.COMPATIBLE_MODE));
        builder.driverName(config.get(JdbcOptions.DRIVER));
        builder.autoCommit(config.get(JdbcOptions.AUTO_COMMIT));
        builder.maxRetries(config.get(JdbcOptions.MAX_RETRIES));
        builder.connectionCheckTimeoutSeconds(config.get(JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC));
        builder.connectionPoolEnabled(config.get(JdbcOptions.CONNECTION_POOL_ENABLED));
        builder.poolSize(config.get(JdbcOptions.POOL_SIZE));
        builder.queryTimeoutSec(config.get(JdbcOptions.QUERY_TIMEOUT_SEC));
        builder.batchSize(config.get(JdbcOptions.BATCH_SIZE));
        config.getOptional(JdbcOptions.USER).ifPresent(builder::username);
        config.getOptional(JdbcOptions.PASSWORD).ifPresent(builder::password);
        config.getOptional(JdbcOptions.PROPERTIES).ifPresent(builder::properties);
        return builder.build();
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getUrl() {
        return url;
    }

    public String getDriverName() {
        return driverName;
    }

    public String getCompatibleMode() {
        return compatibleMode;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public int getConnectionCheckTimeoutSeconds() {
        return connectionCheckTimeoutSeconds;
    }

    public boolean isConnectionPoolEnabled() {
        return connectionPoolEnabled;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public int getQueryTimeoutSec() {
        return queryTimeoutSec;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void applySessionSettings(Connection connection) throws SQLException {
        if (queryTimeoutSec <= 0) {
            return;
        }
        if ("oceanbase".equalsIgnoreCase(compatibleMode)) {
            long timeoutMicros = (long) queryTimeoutSec * 1_000_000L;
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("SET ob_query_timeout = " + timeoutMicros);
            }
            return;
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SET query_timeout = " + queryTimeoutSec);
        } catch (SQLException e) {
            LOG.warn(
                    "Failed to set query_timeout to {} seconds, continuing without session timeout adjustment",
                    queryTimeoutSec,
                    e);
        }
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String dbType;
        private String url;
        private String driverName;
        private String compatibleMode;
        private int connectionCheckTimeoutSeconds =
                JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC.defaultValue();
        private boolean connectionPoolEnabled =
                JdbcOptions.CONNECTION_POOL_ENABLED.defaultValue();
        private int poolSize = JdbcOptions.POOL_SIZE.defaultValue();
        private int queryTimeoutSec = JdbcOptions.QUERY_TIMEOUT_SEC.defaultValue();
        private int maxRetries = JdbcOptions.MAX_RETRIES.defaultValue();
        private String username;
        private String password;
        private String query;
        private boolean autoCommit = JdbcOptions.AUTO_COMMIT.defaultValue();
        private int batchSize = JdbcOptions.BATCH_SIZE.defaultValue();
        private Map<String, String> properties;

        private Builder() {}

        public Builder dbType(String dbType) {
            this.dbType = dbType;
            return this;
        }

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder driverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        public Builder compatibleMode(String compatibleMode) {
            this.compatibleMode = compatibleMode;
            return this;
        }

        public Builder connectionCheckTimeoutSeconds(int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        public Builder connectionPoolEnabled(boolean connectionPoolEnabled) {
            this.connectionPoolEnabled = connectionPoolEnabled;
            return this;
        }

        public Builder poolSize(int poolSize) {
            this.poolSize = poolSize;
            return this;
        }

        public Builder queryTimeoutSec(int queryTimeoutSec) {
            this.queryTimeoutSec = queryTimeoutSec;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder query(String query) {
            this.query = query;
            return this;
        }

        public Builder autoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public JdbcConnectionConfig build() {
            JdbcConnectionConfig jdbcConnectionConfig = new JdbcConnectionConfig();
            jdbcConnectionConfig.batchSize = this.batchSize;
            jdbcConnectionConfig.driverName = this.driverName;
            jdbcConnectionConfig.compatibleMode = this.compatibleMode;
            jdbcConnectionConfig.maxRetries = this.maxRetries;
            jdbcConnectionConfig.password = this.password;
            jdbcConnectionConfig.connectionCheckTimeoutSeconds = this.connectionCheckTimeoutSeconds;
            jdbcConnectionConfig.connectionPoolEnabled = this.connectionPoolEnabled;
            jdbcConnectionConfig.poolSize = this.poolSize;
            jdbcConnectionConfig.queryTimeoutSec = this.queryTimeoutSec;
            jdbcConnectionConfig.url = this.url;
            jdbcConnectionConfig.autoCommit = this.autoCommit;
            jdbcConnectionConfig.username = this.username;
            jdbcConnectionConfig.properties =
                    this.properties == null ? new HashMap<>() : this.properties;
            return jdbcConnectionConfig;
        }
    }
}
