package com.github.ares.connector.jdbc.sink;

import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connector.jdbc.config.JdbcConnectionConfig;
import com.github.ares.connector.jdbc.config.JdbcSinkConfig;
import com.github.ares.connector.jdbc.internal.JdbcOutputFormat;
import com.github.ares.connector.jdbc.internal.JdbcOutputFormatBuilder;
import com.github.ares.connector.jdbc.internal.connection.JdbcConnectionProvider;
import com.github.ares.connector.jdbc.internal.connection.SharedJdbcConnectionProvider;
import com.github.ares.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import com.github.ares.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Holds one JDBC connection (autoCommit=false) and per-SQL batch writers for a PL transaction. */
public class JdbcPlTransactionSession {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcPlTransactionSession.class);

    private JdbcConnectionProvider connectionProvider;
    private SharedJdbcConnectionProvider sharedProvider;
    private String connectionKey;
    private final Map<String, JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>>>
            writers = new LinkedHashMap<>();

    public void write(JdbcSink jdbcSink, AresRow row) {
        try {
            JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>> format =
                    getOrCreateWriter(jdbcSink);
            format.writeRecord(row);
        } catch (AresException e) {
            throw e;
        } catch (Exception e) {
            throw new AresException("Write in transaction failed: " + e.getMessage(), e);
        }
    }

    public void commit() {
        try {
            flushWriters();
            Connection connection = currentConnection();
            if (connection != null && !connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (Exception e) {
            throw new AresException("COMMIT failed: " + e.getMessage(), e);
        }
    }

    public void rollbackQuietly() {
        try {
            flushWriters();
        } catch (Exception e) {
            LOG.warn("Flush before rollback failed", e);
        }
        try {
            Connection connection = currentConnection();
            if (connection != null && !connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (Exception e) {
            LOG.warn("ROLLBACK failed", e);
        }
    }

    /**
     * Close writers and the shared connection. Does not roll back; uncommitted work must already
     * have been committed or rolled back by the caller.
     */
    public void close() {
        for (JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>> format :
                writers.values()) {
            try {
                format.close();
            } catch (Exception e) {
                LOG.warn("Close transactional JDBC writer failed", e);
            }
        }
        writers.clear();
        if (connectionProvider != null) {
            connectionProvider.closeConnection();
            connectionProvider = null;
            sharedProvider = null;
            connectionKey = null;
        }
    }

    private JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>> getOrCreateWriter(
            JdbcSink jdbcSink) throws Exception {
        JdbcSinkConfig sinkConfig = jdbcSink.getJdbcSinkConfig();
        JdbcConnectionConfig jdbcConfig = sinkConfig.getJdbcConnectionConfig();
        String key = connectionKey(jdbcConfig);
        ensureConnection(jdbcConfig, key);

        String writerKey = sinkConfig.getSimpleSql();
        JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>> format =
                writers.get(writerKey);
        if (format == null) {
            format =
                    new JdbcOutputFormatBuilder(
                                    jdbcSink.dialect(),
                                    sharedProvider,
                                    sinkConfig,
                                    jdbcSink.getAresRowType())
                            .build();
            format.open();
            writers.put(writerKey, format);
        }
        return format;
    }

    private void ensureConnection(JdbcConnectionConfig jdbcConfig, String key)
            throws SQLException, ClassNotFoundException {
        if (connectionProvider == null) {
            connectionProvider = new SimpleJdbcConnectionProvider(jdbcConfig);
            sharedProvider = new SharedJdbcConnectionProvider(connectionProvider);
            Connection connection = connectionProvider.getOrEstablishConnection();
            connection.setAutoCommit(false);
            connectionKey = key;
            return;
        }
        if (!key.equals(connectionKey)) {
            throw new AresException(
                    "START TRANSACTION only supports one JDBC datasource, got a different url/user");
        }
    }

    private void flushWriters() throws Exception {
        for (JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>> format :
                writers.values()) {
            format.flush();
        }
    }

    private Connection currentConnection() {
        return connectionProvider == null ? null : connectionProvider.getConnection();
    }

    private static String connectionKey(JdbcConnectionConfig config) {
        return config.getUrl() + "|" + config.getUsername().orElse("");
    }
}
