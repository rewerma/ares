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

import com.github.ares.api.source.Collector;
import com.github.ares.api.source.SourceReader;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.connctor.jdbc.config.JdbcSourceConfig;
import com.github.ares.connctor.jdbc.internal.JdbcInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

public class JdbcSourceReader implements SourceReader<AresRow, JdbcSourceSplit> {
    private static final Logger log = LoggerFactory.getLogger(JdbcSourceReader.class);
    private final Context context;
    private final JdbcInputFormat inputFormat;
    private final Deque<JdbcSourceSplit> splits = new ConcurrentLinkedDeque<>();
    private volatile boolean noMoreSplit;

    public JdbcSourceReader(
            Context context, JdbcSourceConfig config, Map<TablePath, AresRowType> tables) {
        this.inputFormat = new JdbcInputFormat(config, tables);
        this.context = context;
    }

    @Override
    public void open() throws Exception {
        inputFormat.openInputFormat();
    }

    @Override
    public void close() throws IOException {
        inputFormat.closeInputFormat();
    }

    @Override
    @SuppressWarnings("magicnumber")
    public void pollNext(Collector<AresRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            JdbcSourceSplit split = splits.poll();
            if (null != split) {
                try {
                    inputFormat.open(split);
                    while (!inputFormat.reachedEnd()) {
                        AresRow AresRow = inputFormat.nextRecord();
                        output.collect(AresRow);
                    }
                } finally {
                    inputFormat.close();
                }
            } else if (noMoreSplit && splits.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded jdbc source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(1000L);
            }
        }
    }

    @Override
    public List<JdbcSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(splits);
    }

    @Override
    public void addSplits(List<JdbcSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    // @Override
    // public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // }
}
