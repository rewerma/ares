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
import com.github.ares.common.utils.IsolatedClassLoader;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.common.utils.SerializationUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SparkDataSourceWriter<StateT, CommitInfoT, AggregatedCommitInfoT>
        implements DataSourceWriter {

    protected final AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink;

//    @Nullable protected final SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT>
//            sinkAggregatedCommitter;

    protected final CatalogTable catalogTable;


    public SparkDataSourceWriter(
            AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            CatalogTable catalogTable)
            throws IOException {
        this.sink = sink;
        this.catalogTable = catalogTable;
//        this.sinkAggregatedCommitter = sink.createAggregatedCommitter().orElse(null);
//        if (sinkAggregatedCommitter != null) {
//            sinkAggregatedCommitter.init();
//        }
    }

    private SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> getAggregatedCommitter() {
        try {
            URL jarUrl = sink.getClass().getProtectionDomain().getCodeSource().getLocation();
            if (jarUrl.getFile().endsWith(".jar")) {
                String sinkSerialization = SerializationUtils.objectToString(sink);
                ClassLoader isolatedLoader =
                        new IsolatedClassLoader(new URL[]{jarUrl}, getClass().getClassLoader());
                byte[] sinkBytes = Base64.getDecoder().decode(sinkSerialization);
                AresSink<AresRow, StateT, CommitInfoT, AggregatedCommitInfoT> aresSink
                        = SerializationUtils.deserialize(sinkBytes, isolatedLoader);

                return aresSink.createAggregatedCommitter().orElse(null);
            }
            return sink.createAggregatedCommitter().orElse(null);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new SparkDataWriterFactory<>(sink, catalogTable);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> sinkAggregatedCommitter = getAggregatedCommitter();
        if (sinkAggregatedCommitter != null) {
            try {
                List<CommitInfoT> commitInfos =
                        Arrays.stream(messages)
                                .map(m -> ((SparkWriterCommitMessage<CommitInfoT>) m).getMessage())
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());
                String jsonStr = JsonUtils.toJsonString(commitInfos);
                sinkAggregatedCommitter.commit(jsonStr);
            } catch (IOException e) {
                throw new RuntimeException("SinkAggregatedCommitter commit failed in driver", e);
            }
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> sinkAggregatedCommitter = getAggregatedCommitter();
        if (sinkAggregatedCommitter != null) {
            try {
                List<CommitInfoT> commitInfos =
                        Arrays.stream(messages)
                                .map(m -> ((SparkWriterCommitMessage<CommitInfoT>) m).getMessage())
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());
                String jsonStr = JsonUtils.toJsonString(commitInfos);
                sinkAggregatedCommitter.abort(jsonStr);
            } catch (Exception e) {
                throw new RuntimeException("SinkAggregatedCommitter abort failed in driver", e);
            }
        }
    }
}
