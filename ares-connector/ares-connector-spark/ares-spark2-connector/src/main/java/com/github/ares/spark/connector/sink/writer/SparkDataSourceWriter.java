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
import com.github.ares.api.sink.MultiTableResourceManager;
import com.github.ares.api.sink.SinkAggregatedCommitter;
import com.github.ares.api.sink.SupportResourceShare;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SparkDataSourceWriter<StateT, CommitInfoT, AggregatedCommitInfoT>
        implements DataSourceWriter {

    protected final AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink;

    @Nullable protected final SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT>
            sinkAggregatedCommitter;

    protected final CatalogTable catalogTable;

    private MultiTableResourceManager resourceManager;

    public SparkDataSourceWriter(
            AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            CatalogTable catalogTable)
            throws IOException {
        this.sink = sink;
        this.catalogTable = catalogTable;
        this.sinkAggregatedCommitter = sink.createAggregatedCommitter().orElse(null);
        if (sinkAggregatedCommitter != null) {
            // TODO close it
            if (this.sinkAggregatedCommitter instanceof SupportResourceShare) {
                resourceManager =
                        ((SupportResourceShare) this.sinkAggregatedCommitter)
                                .initMultiTableResourceManager(1, 1);
            }
            sinkAggregatedCommitter.init();
            if (resourceManager != null) {
                ((SupportResourceShare) this.sinkAggregatedCommitter)
                        .setMultiTableResourceManager(resourceManager, 0);
            }
        }
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new SparkDataWriterFactory<>(sink, catalogTable);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        if (sinkAggregatedCommitter != null) {
            try {
                sinkAggregatedCommitter.commit(combineCommitMessage(messages));
            } catch (IOException e) {
                throw new RuntimeException("SinkAggregatedCommitter commit failed in driver", e);
            }
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        if (sinkAggregatedCommitter != null) {
            try {
                sinkAggregatedCommitter.abort(combineCommitMessage(messages));
            } catch (Exception e) {
                throw new RuntimeException("SinkAggregatedCommitter abort failed in driver", e);
            }
        }
    }

    /** {@link SparkDataWriter#commit()} */
    @SuppressWarnings("unchecked")
    private @Nonnull List<AggregatedCommitInfoT> combineCommitMessage(
            WriterCommitMessage[] messages) {
        if (sinkAggregatedCommitter == null || messages.length == 0) {
            return Collections.emptyList();
        }
        List<CommitInfoT> commitInfos =
                Arrays.stream(messages)
                        .map(m -> ((SparkWriterCommitMessage<CommitInfoT>) m).getMessage())
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        return Collections.singletonList(sinkAggregatedCommitter.combine(commitInfos));
    }
}
