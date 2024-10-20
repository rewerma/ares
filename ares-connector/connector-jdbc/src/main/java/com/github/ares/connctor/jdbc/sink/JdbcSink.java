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


import com.github.ares.api.common.JobContext;
import com.github.ares.api.sink.AresSink;
import com.github.ares.api.sink.SinkAggregatedCommitter;
import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.config.JdbcSinkConfig;
import com.github.ares.connctor.jdbc.internal.connection.JdbcConnectionProvider;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.source.ChunkSplitter;
import com.github.ares.connctor.jdbc.state.JdbcAggregatedCommitInfo;
import com.github.ares.connctor.jdbc.state.JdbcSinkState;
import com.github.ares.connctor.jdbc.state.XidInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Optional;


public class JdbcSink
        implements AresSink<AresRow, JdbcSinkState, XidInfo, JdbcAggregatedCommitInfo> {
    private static final Logger log = LoggerFactory.getLogger(JdbcSink.class);

    private AresRowType aresRowType;

    private JobContext jobContext;

    private final JdbcSinkConfig jdbcSinkConfig;

    private final JdbcDialect dialect;

    private final ReadonlyConfig config;

    public JdbcSink(
            ReadonlyConfig config,
            JdbcSinkConfig jdbcSinkConfig,
            JdbcDialect dialect,
            AresRowType rowType) {
        this.config = config;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.dialect = dialect;
        this.aresRowType = rowType;
    }

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public SinkWriter<AresRow, XidInfo, JdbcSinkState> createWriter(
            SinkWriter.Context context) {
        SinkWriter<AresRow, XidInfo, JdbcSinkState> sinkWriter;
        if (jdbcSinkConfig.isExactlyOnce()) {
            sinkWriter =
                    new JdbcExactlyOnceSinkWriter(
                            context,
                            jobContext,
                            dialect,
                            jdbcSinkConfig,
                            aresRowType,
                            new ArrayList<>());
        } else {
            sinkWriter =
                    new JdbcSinkWriter(dialect, jdbcSinkConfig, aresRowType, null);
        }
        return sinkWriter;
    }

    @Override
    public Optional<SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo>>
    createAggregatedCommitter() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new JdbcSinkAggregatedCommitter(jdbcSinkConfig));
        }
        return Optional.empty();
    }

    @Override
    public void truncateTable(String tableName) {
        JdbcConnectionProvider connectionProvider =
                dialect.getJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
        try (Connection conn = connectionProvider.getOrEstablishConnection();
             PreparedStatement pStmt = conn.prepareStatement(jdbcSinkConfig.getSimpleSql())) {
            pStmt.execute();
        } catch (Exception e) {
            throw new AresException(String.format("Truncate table failed: %s, cause: %s", tableName, e.getMessage()));
        }
    }
}
