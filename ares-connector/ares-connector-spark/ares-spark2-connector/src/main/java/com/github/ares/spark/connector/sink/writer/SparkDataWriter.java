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

import com.github.ares.api.sink.MultiTableResourceManager;
import com.github.ares.api.sink.SinkCommitter;
import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.sink.SupportResourceShare;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.connector.serialization.RowConverter;
import com.github.ares.spark.connector.serialization.InternalRowConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

public class SparkDataWriter<CommitInfoT, StateT> implements DataWriter<InternalRow> {
    private Logger log = LoggerFactory.getLogger(SparkDataWriter.class);

    private final SinkWriter<AresRow, CommitInfoT, StateT> sinkWriter;

    @Nullable private final SinkCommitter<CommitInfoT> sinkCommitter;
    private final RowConverter<InternalRow> rowConverter;
    private CommitInfoT latestCommitInfoT;
    private long epochId;
    private MultiTableResourceManager resourceManager;

    SparkDataWriter(
            SinkWriter<AresRow, CommitInfoT, StateT> sinkWriter,
            @Nullable SinkCommitter<CommitInfoT> sinkCommitter,
            AresDataType<?> dataType,
            long epochId) {
        this.sinkWriter = sinkWriter;
        this.sinkCommitter = sinkCommitter;
        this.rowConverter = new InternalRowConverter(dataType);
        this.epochId = epochId == 0 ? 1 : epochId;
        initResourceManger();
    }

    @Override
    public void write(InternalRow record) throws IOException {
        sinkWriter.write(rowConverter.reconvert(record));
    }

    private void initResourceManger() {
        if (sinkWriter instanceof SupportResourceShare) {
            resourceManager =
                    ((SupportResourceShare) sinkWriter).initMultiTableResourceManager(1, 1);
            ((SupportResourceShare) sinkWriter).setMultiTableResourceManager(resourceManager, 0);
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        // We combine the prepareCommit and commit in this method.
        // If this method fails, we need to rollback the transaction in the abort method.
        // 1. prepareCommit fails:
        //   1.1. We don't have the commit info, we need to execute the sinkWriter#abort to rollback
        // the transaction.
        // 2. commit fails
        //   2.1. We have the commit info, we need to execute the sinkCommitter#abort to rollback
        // the transaction.
        Optional<CommitInfoT> commitInfoTOptional = sinkWriter.prepareCommit();
        commitInfoTOptional.ifPresent(commitInfoT -> latestCommitInfoT = commitInfoT);
        sinkWriter.snapshotState(epochId++);
        if (sinkCommitter != null) {
            if (latestCommitInfoT == null) {
                sinkCommitter.commit(Collections.emptyList());
            } else {
                sinkCommitter.commit(Collections.singletonList(latestCommitInfoT));
            }
        }
        SparkWriterCommitMessage<CommitInfoT> sparkWriterCommitMessage =
                new SparkWriterCommitMessage<>(latestCommitInfoT);
        cleanCommitInfo();
        sinkWriter.close();
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
        return sparkWriterCommitMessage;
    }

    @Override
    public void abort() throws IOException {
        sinkWriter.abortPrepare();
        if (sinkCommitter != null) {
            if (latestCommitInfoT == null) {
                sinkCommitter.abort(Collections.emptyList());
            } else {
                sinkCommitter.abort(Collections.singletonList(latestCommitInfoT));
            }
        }
        cleanCommitInfo();
    }

    private void cleanCommitInfo() {
        latestCommitInfoT = null;
    }
}
