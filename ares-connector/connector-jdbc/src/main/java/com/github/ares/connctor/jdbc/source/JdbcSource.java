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

package com.github.ares.connctor.jdbc.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.Boundedness;
import com.github.ares.api.source.SourceReader;
import com.github.ares.api.source.SourceSplitEnumerator;
import com.github.ares.api.source.SupportColumnProjection;
import com.github.ares.api.source.SupportParallelism;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.serialization.Serializer;
import com.github.ares.connctor.jdbc.config.JdbcSourceConfig;
import com.github.ares.connctor.jdbc.state.JdbcSourceState;
import com.github.ares.connctor.jdbc.utils.JdbcCatalogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JdbcSource
        implements AresSource<AresRow, JdbcSourceSplit, JdbcSourceState>,
        SupportParallelism,
        SupportColumnProjection {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    private final JdbcSourceConfig jdbcSourceConfig;
    private final Map<TablePath, JdbcSourceTable> jdbcSourceTables;

    public JdbcSource(JdbcSourceConfig jdbcSourceConfig) {
        this.jdbcSourceConfig = jdbcSourceConfig;
        try {
            this.jdbcSourceTables =
                    JdbcCatalogUtils.getTables(
                            jdbcSourceConfig.getJdbcConnectionConfig(),
                            jdbcSourceConfig.getTableConfigList());
        } catch (Exception e) {
            throw new AresException(e);
        }
    }

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return jdbcSourceTables.values().stream()
                .map(JdbcSourceTable::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<AresRow, JdbcSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        Map<TablePath, AresRowType> tables = new HashMap<>();
        for (TablePath tablePath : jdbcSourceTables.keySet()) {
            AresRowType rowType =
                    jdbcSourceTables
                            .get(tablePath)
                            .getCatalogTable()
                            .getTableSchema()
                            .toPhysicalRowDataType();
            tables.put(tablePath, rowType);
        }
        return new JdbcSourceReader(readerContext, jdbcSourceConfig, tables);
    }

    @Override
    public Serializer<JdbcSourceSplit> getSplitSerializer() {
        return AresSource.super.getSplitSerializer();
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> createEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext) throws Exception {
        return new JdbcSourceSplitEnumerator(
                enumeratorContext, jdbcSourceConfig, jdbcSourceTables, null);
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext,
            JdbcSourceState checkpointState)
            throws Exception {
        return new JdbcSourceSplitEnumerator(
                enumeratorContext, jdbcSourceConfig, jdbcSourceTables, checkpointState);
    }
}
