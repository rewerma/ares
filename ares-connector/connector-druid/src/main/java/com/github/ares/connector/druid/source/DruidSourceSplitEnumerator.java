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

import com.github.ares.api.source.SourceSplitEnumerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DruidSourceSplitEnumerator
        implements SourceSplitEnumerator<DruidSourceSplit, ArrayList<DruidSourceSplit>> {

    private final SourceSplitEnumerator.Context<DruidSourceSplit> context;
    private final String sql;
    private boolean assigned;

    public DruidSourceSplitEnumerator(
            SourceSplitEnumerator.Context<DruidSourceSplit> context, String sql) {
        this.context = context;
        this.sql = sql;
    }

    @Override
    public void open() {}

    @Override
    public void run() {
        if (assigned) {
            return;
        }
        DruidSourceSplit split = new DruidSourceSplit("druid-0", sql);
        int reader = 0;
        if (!context.registeredReaders().isEmpty()) {
            reader = context.registeredReaders().iterator().next();
        }
        List<DruidSourceSplit> splits = new ArrayList<DruidSourceSplit>();
        splits.add(split);
        context.assignSplit(reader, splits);
        context.signalNoMoreSplits(reader);
        assigned = true;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<DruidSourceSplit> splits, int subtaskId) {
        context.assignSplit(subtaskId, splits);
        context.signalNoMoreSplits(subtaskId);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return assigned ? 0 : 1;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {}

    @Override
    public void registerReader(int subtaskId) {}

    @Override
    public ArrayList<DruidSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<DruidSourceSplit>();
    }
}
