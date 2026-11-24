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

package com.github.ares.connector.druid.source;

import com.github.ares.api.common.JobContext;
import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.Boundedness;
import com.github.ares.api.source.SourceReader;
import com.github.ares.api.source.SourceSplitEnumerator;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.TableIdentifier;
import com.github.ares.api.table.catalog.TableSchema;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.druid.DruidSqlClient;
import com.github.ares.connector.druid.DruidTypeMapper;
import com.github.ares.connector.druid.config.DruidOptions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DruidSource
        implements AresSource<AresRow, DruidSourceSplit, ArrayList<DruidSourceSplit>> {

    private final DruidSourceConfig config;
    private JobContext jobContext;

    public DruidSource(ReadonlyConfig options) {
        this.config = new DruidSourceConfig(options);
    }

    public DruidSource(DruidSourceConfig config) {
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        try {
            DruidSqlClient.DruidSqlResult result =
                    new DruidSqlClient()
                            .query(
                                    config.getUrl(),
                                    config.getUser(),
                                    config.getPassword(),
                                    config.resolveSql(),
                                    config.getTimeoutMs());
            TableSchema schema =
                    DruidTypeMapper.schema(
                            result.getColumns(), result.getTypes(), result.getRows());
            String table = config.getTableName() == null ? "druid_query" : config.getTableName();
            CatalogTable catalogTable =
                    CatalogTable.of(
                            TableIdentifier.of(DruidOptions.CONNECTOR_IDENTIFIER, "druid", table),
                            schema,
                            Collections.<String, String>emptyMap(),
                            Collections.<String>emptyList(),
                            "Apache Druid SQL HTTP");
            return Collections.singletonList(catalogTable);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to read Apache Druid schema: " + e.getMessage(), e);
        }
    }

    @Override
    public SourceSplitEnumerator<DruidSourceSplit, ArrayList<DruidSourceSplit>> createEnumerator(
            SourceSplitEnumerator.Context<DruidSourceSplit> enumeratorContext) {
        return new DruidSourceSplitEnumerator(enumeratorContext, config.resolveSql());
    }

    @Override
    public SourceSplitEnumerator<DruidSourceSplit, ArrayList<DruidSourceSplit>> restoreEnumerator(
            SourceSplitEnumerator.Context<DruidSourceSplit> enumeratorContext,
            ArrayList<DruidSourceSplit> checkpointState) {
        return createEnumerator(enumeratorContext);
    }

    @Override
    public SourceReader<AresRow, DruidSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new DruidSourceReader(readerContext, config);
    }

    @Override
    public String getPluginName() {
        return DruidOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}
