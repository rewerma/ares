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

package com.github.ares.spark.connector.sink.writer;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.sink.SinkAggregatedCommitter;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.spark.connector.sink.SinkAggregatedCommitterLoader;
import com.github.ares.spark.connector.statistic.JobStatisticInformation;
import com.github.ares.spark.connector.statistic.WriterStatistic;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SparkDataSourceWriter<StateT, CommitInfoT, AggregatedCommitInfoT>
        implements DataSourceWriter {

    protected final AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink;

//    @Nullable protected final SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT>
//            sinkAggregatedCommitter;

    protected final CatalogTable catalogTable;

    private final long startTimeMillis;


    public SparkDataSourceWriter(
            AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            CatalogTable catalogTable)
            throws IOException {
        this.sink = sink;
        this.catalogTable = catalogTable;
        this.startTimeMillis = System.currentTimeMillis();
//        this.sinkAggregatedCommitter = sink.createAggregatedCommitter().orElse(null);
//        if (sinkAggregatedCommitter != null) {
//            sinkAggregatedCommitter.init();
//        }
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new SparkDataWriterFactory<>(sink, catalogTable);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        WriterStatistic totalStatistic = aggregateStatistic(messages);
        List<CommitInfoT> commitInfos = collectCommitInfos(messages);
        SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> sinkAggregatedCommitter =
                SinkAggregatedCommitterLoader.load(sink, getClass().getClassLoader());
        SinkAggregatedCommitterLoader.ensureCommitterPresentIfNeeded(
                sinkAggregatedCommitter, commitInfos.size());
        if (sinkAggregatedCommitter != null) {
            try {
                sinkAggregatedCommitter.commit(JsonUtils.toJsonString(commitInfos));
            } catch (IOException e) {
                throw new RuntimeException("SinkAggregatedCommitter commit failed in driver", e);
            }
        }
        JobStatisticInformation.log(
                startTimeMillis,
                totalStatistic.getReadCount(),
                totalStatistic.getWriteCount(),
                totalStatistic.getFailedCount());
    }

    private WriterStatistic aggregateStatistic(WriterCommitMessage[] messages) {
        WriterStatistic totalStatistic = new WriterStatistic();
        if (messages == null) {
            return totalStatistic;
        }
        for (WriterCommitMessage message : messages) {
            if (message instanceof SparkWriterCommitMessage) {
                totalStatistic.merge(((SparkWriterCommitMessage<?>) message).getStatistic());
            }
        }
        return totalStatistic;
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        List<CommitInfoT> commitInfos = collectCommitInfos(messages);
        SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> sinkAggregatedCommitter =
                SinkAggregatedCommitterLoader.load(sink, getClass().getClassLoader());
        SinkAggregatedCommitterLoader.ensureCommitterPresentIfNeeded(
                sinkAggregatedCommitter, commitInfos.size());
        if (sinkAggregatedCommitter != null) {
            try {
                sinkAggregatedCommitter.abort(JsonUtils.toJsonString(commitInfos));
            } catch (Exception e) {
                throw new RuntimeException("SinkAggregatedCommitter abort failed in driver", e);
            }
        }
    }

    private List<CommitInfoT> collectCommitInfos(WriterCommitMessage[] messages) {
        if (messages == null) {
            return Arrays.asList();
        }
        return Arrays.stream(messages)
                .map(m -> ((SparkWriterCommitMessage<CommitInfoT>) m).getMessage())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
