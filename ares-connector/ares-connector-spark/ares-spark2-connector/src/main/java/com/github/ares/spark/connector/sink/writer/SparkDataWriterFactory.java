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
import com.github.ares.api.sink.DefaultSinkWriterContext;
import com.github.ares.api.sink.SinkCommitter;
import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;

import java.io.IOException;

public class SparkDataWriterFactory<CommitInfoT, StateT> implements DataWriterFactory<InternalRow> {

    private final AresSink<AresRow, StateT, CommitInfoT, ?> sink;
    private final CatalogTable catalogTable;

    SparkDataWriterFactory(
            AresSink<AresRow, StateT, CommitInfoT, ?> sink, CatalogTable catalogTable) {
        this.sink = sink;
        this.catalogTable = catalogTable;
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
        com.github.ares.api.sink.SinkWriter.Context context =
                new DefaultSinkWriterContext((int) taskId);
        SinkWriter<AresRow, CommitInfoT, StateT> writer;
        SinkCommitter<CommitInfoT> committer;
        try {
            writer = sink.createWriter(context);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create SinkWriter.", e);
        }
        try {
            committer = sink.createCommitter().orElse(null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create SinkCommitter.", e);
        }
        return new SparkDataWriter<>(
                writer, committer, catalogTable.getAresRowType(), epochId);
    }
}
