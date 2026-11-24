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

import com.github.ares.api.source.Boundedness;
import com.github.ares.api.source.Collector;
import com.github.ares.api.source.SourceReader;
import com.github.ares.api.table.catalog.TableSchema;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.connector.druid.DruidSqlClient;
import com.github.ares.connector.druid.DruidTypeMapper;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

public class DruidSourceReader implements SourceReader<AresRow, DruidSourceSplit> {

    private final SourceReader.Context context;
    private final DruidSourceConfig config;
    private final DruidSqlClient sqlClient;
    private final Deque<DruidSourceSplit> splits = new ConcurrentLinkedDeque<DruidSourceSplit>();
    private volatile boolean noMoreSplit;

    public DruidSourceReader(SourceReader.Context context, DruidSourceConfig config) {
        this.context = context;
        this.config = config;
        this.sqlClient = new DruidSqlClient();
    }

    @Override
    public void open() {}

    @Override
    public void close() {}

    @Override
    public void pollNext(Collector<AresRow> output) throws Exception {
        DruidSourceSplit split = splits.poll();
        if (split != null) {
            DruidSqlClient.DruidSqlResult result =
                    sqlClient.query(
                            config.getUrl(),
                            config.getUser(),
                            config.getPassword(),
                            split.getSql(),
                            config.getTimeoutMs());
            TableSchema schema =
                    DruidTypeMapper.schema(
                            result.getColumns(), result.getTypes(), result.getRows());
            synchronized (output.getCheckpointLock()) {
                for (Map<String, String> row : result.getRows()) {
                    Object[] values = new Object[result.getColumns().size()];
                    for (int i = 0; i < result.getColumns().size(); i++) {
                        String name = result.getColumns().get(i);
                        String raw = row == null ? null : row.get(name);
                        values[i] =
                                DruidTypeMapper.convert(
                                        raw, schema.getColumns().get(i).getDataType());
                    }
                    output.collect(new AresRow(values));
                }
            }
        }
        if (noMoreSplit
                && splits.isEmpty()
                && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<DruidSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<DruidSourceSplit>(splits);
    }

    @Override
    public void addSplits(List<DruidSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }
}
