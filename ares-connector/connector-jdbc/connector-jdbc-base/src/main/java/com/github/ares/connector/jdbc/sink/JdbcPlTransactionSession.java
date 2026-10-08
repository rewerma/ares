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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds one JDBC connection ({@code autoCommit=false}) per datasource and per-SQL batch writers for
 * a PL transaction. {@code COMMIT} and {@code ROLLBACK} are applied to every datasource in the
 * segment. Each datasource has its own local transaction.
 */
public class JdbcPlTransactionSession {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcPlTransactionSession.class);

    private final Map<String, SourceSession> sources = new LinkedHashMap<>();

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
        } catch (Exception e) {
            throw new AresException("COMMIT failed: " + e.getMessage(), e);
        }
        List<String> committed = new ArrayList<>();
        for (Map.Entry<String, SourceSession> entry : sources.entrySet()) {
            try {
                entry.getValue().commit();
                committed.add(entry.getKey());
            } catch (Exception e) {
                rollbackExcept(committed);
                throw new AresException(
                        "COMMIT failed for JDBC datasource "
                                + entry.getKey()
                                + ". Datasources already committed: "
                                + committed
                                + ". "
                                + e.getMessage(),
                        e);
            }
        }
    }

    public void rollbackQuietly() {
        try {
            flushWriters();
        } catch (Exception e) {
            LOG.warn("Flush before rollback failed", e);
        }
        for (SourceSession source : sources.values()) {
            source.rollbackQuietly();
        }
    }

    /**
     * Close writers and every datasource connection. Does not roll back; uncommitted work must
     * already have been committed or rolled back by the caller.
     */
    public void close() {
        for (SourceSession source : sources.values()) {
            source.closeWriters();
        }
        for (SourceSession source : sources.values()) {
            source.closeConnection();
        }
        sources.clear();
    }

    private JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>> getOrCreateWriter(
            JdbcSink jdbcSink) throws Exception {
        JdbcSinkConfig sinkConfig = jdbcSink.getJdbcSinkConfig();
        JdbcConnectionConfig jdbcConfig = sinkConfig.getJdbcConnectionConfig();
        String key = connectionKey(jdbcConfig);
        SourceSession source = sources.get(key);
        if (source == null) {
            source = new SourceSession(jdbcConfig);
            sources.put(key, source);
        }
        return source.writer(jdbcSink);
    }

    private void flushWriters() throws Exception {
        for (SourceSession source : sources.values()) {
            source.flush();
        }
    }

    private void rollbackExcept(List<String> committed) {
        for (Map.Entry<String, SourceSession> entry : sources.entrySet()) {
            if (committed.contains(entry.getKey())) {
                continue;
            }
            entry.getValue().rollbackQuietly();
        }
    }

    private static String connectionKey(JdbcConnectionConfig config) {
        return config.getUrl() + "|" + config.getUsername().orElse("");
    }

    private static final class SourceSession {
        private final JdbcConnectionProvider connectionProvider;
        private final SharedJdbcConnectionProvider sharedProvider;
        private final Map<String, JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>>>
                writers = new LinkedHashMap<>();

        private SourceSession(JdbcConnectionConfig jdbcConfig)
                throws SQLException, ClassNotFoundException {
            connectionProvider = new SimpleJdbcConnectionProvider(jdbcConfig);
            sharedProvider = new SharedJdbcConnectionProvider(connectionProvider);
            Connection connection = connectionProvider.getOrEstablishConnection();
            connection.setAutoCommit(false);
        }

        private JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>> writer(
                JdbcSink jdbcSink) throws Exception {
            JdbcSinkConfig sinkConfig = jdbcSink.getJdbcSinkConfig();
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

        private void flush() throws Exception {
            for (JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>> format :
                    writers.values()) {
                format.flush();
            }
        }

        private void commit() throws SQLException {
            Connection connection = connectionProvider.getConnection();
            if (connection != null && !connection.getAutoCommit()) {
                connection.commit();
            }
        }

        private void rollbackQuietly() {
            try {
                Connection connection = connectionProvider.getConnection();
                if (connection != null && !connection.getAutoCommit()) {
                    connection.rollback();
                }
            } catch (Exception e) {
                LOG.warn("ROLLBACK failed", e);
            }
        }

        private void closeWriters() {
            for (JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>> format :
                    writers.values()) {
                try {
                    format.close();
                } catch (Exception e) {
                    LOG.warn("Close transactional JDBC writer failed", e);
                }
            }
            writers.clear();
        }

        private void closeConnection() {
            connectionProvider.closeConnection();
        }
    }
}
