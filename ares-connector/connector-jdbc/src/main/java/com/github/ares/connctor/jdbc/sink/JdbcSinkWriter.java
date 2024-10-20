/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.ares.connctor.jdbc.sink;

import com.github.ares.api.sink.MultiTableResourceManager;
import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.config.JdbcSinkConfig;
import com.github.ares.connctor.jdbc.internal.JdbcOutputFormat;
import com.github.ares.connctor.jdbc.internal.JdbcOutputFormatBuilder;
import com.github.ares.connctor.jdbc.internal.connection.JdbcConnectionProvider;
import com.github.ares.connctor.jdbc.internal.connection.SimpleJdbcConnectionPoolProviderProxy;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.executor.JdbcBatchStatementExecutor;
import com.github.ares.connctor.jdbc.state.JdbcSinkState;
import com.github.ares.connctor.jdbc.state.XidInfo;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

public class JdbcSinkWriter
        implements SinkWriter<AresRow, XidInfo, JdbcSinkState>,
        SupportMultiTableSinkWriter<ConnectionPoolManager> {
    private static final Logger log = LoggerFactory.getLogger(JdbcSinkWriter.class);
    private JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>> outputFormat;
    private final JdbcDialect dialect;
    private final AresRowType rowType;
    private JdbcConnectionProvider connectionProvider;
    private transient boolean isOpen;
    private final Integer primaryKeyIndex;
    private final JdbcSinkConfig jdbcSinkConfig;

    private final LongAdder writeCounter;

    public JdbcSinkWriter(
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            AresRowType rowType,
            Integer primaryKeyIndex) {
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.dialect = dialect;
        this.rowType = rowType;
        this.primaryKeyIndex = primaryKeyIndex;
        this.connectionProvider =
                dialect.getJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
        this.outputFormat =
                new JdbcOutputFormatBuilder(dialect, connectionProvider, jdbcSinkConfig, rowType)
                        .build();
        this.writeCounter = new LongAdder();
    }

    @Override
    public MultiTableResourceManager<ConnectionPoolManager> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        HikariDataSource ds = new HikariDataSource();
        ds.setIdleTimeout(30 * 1000);
        ds.setMaximumPoolSize(queueSize);
        ds.setJdbcUrl(jdbcSinkConfig.getJdbcConnectionConfig().getUrl());
        if (jdbcSinkConfig.getJdbcConnectionConfig().getUsername().isPresent()) {
            ds.setUsername(jdbcSinkConfig.getJdbcConnectionConfig().getUsername().get());
        }
        if (jdbcSinkConfig.getJdbcConnectionConfig().getPassword().isPresent()) {
            ds.setPassword(jdbcSinkConfig.getJdbcConnectionConfig().getPassword().get());
        }
        ds.setAutoCommit(jdbcSinkConfig.getJdbcConnectionConfig().isAutoCommit());
        return new JdbcMultiTableResourceManager(new ConnectionPoolManager(ds));
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<ConnectionPoolManager> multiTableResourceManager,
            int queueIndex) {
        connectionProvider.closeConnection();
        this.connectionProvider =
                new SimpleJdbcConnectionPoolProviderProxy(
                        multiTableResourceManager.getSharedResource().get(),
                        jdbcSinkConfig.getJdbcConnectionConfig(),
                        queueIndex);
        this.outputFormat =
                new JdbcOutputFormatBuilder(dialect, connectionProvider, jdbcSinkConfig, rowType)
                        .build();
    }

    @Override
    public Optional<Integer> primaryKey() {
        return primaryKeyIndex != null ? Optional.of(primaryKeyIndex) : Optional.empty();
    }

    private void tryOpen() throws IOException {
        if (!isOpen) {
            isOpen = true;
            outputFormat.open();
        }
    }

    @Override
    public List<JdbcSinkState> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void write(AresRow element) throws IOException {
        tryOpen();
        outputFormat.writeRecord(element);
        writeCounter.increment();
    }

    @Override
    public Optional<XidInfo> prepareCommit() throws IOException {
        tryOpen();
        outputFormat.checkFlushException();
        outputFormat.flush();
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new AresException(
                    "commit failed," + e.getMessage(),
                    e);
        }
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
    }

    @Override
    public void close() throws IOException {
        tryOpen();
        outputFormat.flush();
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new AresException(
                    "unable to close JDBC sink write",
                    e);
        }
        outputFormat.close();
        log.info("Data write count: {}", writeCounter.sum());
    }
}
