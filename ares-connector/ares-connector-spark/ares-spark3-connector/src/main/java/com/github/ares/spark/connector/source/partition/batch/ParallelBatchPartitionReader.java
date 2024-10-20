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

package com.github.ares.spark.connector.source.partition.batch;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SourceSplit;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.engine.Handover;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connector.source.BaseSourceFunction;
import com.github.ares.connector.source.ParallelSource;
import com.github.ares.connector.utils.ThreadPoolExecutorFactory;
import com.github.ares.spark.connector.serialization.InternalRowCollector;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class ParallelBatchPartitionReader {
    private static final Logger log = LoggerFactory.getLogger(ParallelBatchPartitionReader.class);

    protected static final Integer INTERVAL = 100;

    protected final AresSource<AresRow, ?, ?> source;
    protected final Integer parallelism;
    protected final Integer subtaskId;

    protected final ExecutorService executorService;
    protected final Handover<InternalRow> handover;

    protected final Object checkpointLock = new Object();

    protected volatile boolean running = true;
    protected volatile boolean prepare = true;

    protected volatile BaseSourceFunction<AresRow> internalSource;
    protected volatile InternalRowCollector internalRowCollector;
    private final Map<String, String> envOptions;

    public ParallelBatchPartitionReader(
            AresSource<AresRow, ?, ?> source,
            Integer parallelism,
            Integer subtaskId,
            Map<String, String> envOptions) {
        this.source = source;
        this.parallelism = parallelism;
        this.subtaskId = subtaskId;
        this.executorService =
                ThreadPoolExecutorFactory.createScheduledThreadPoolExecutor(
                        1, getEnumeratorThreadName());
        this.handover = new Handover<>();
        this.envOptions = envOptions;
    }

    protected String getEnumeratorThreadName() {
        return String.format("parallel-split-enumerator-executor-%s", subtaskId);
    }

    public boolean next() throws IOException {
        prepare();
        while (running && handover.isEmpty()) {
            try {
                Thread.sleep(INTERVAL);
            } catch (InterruptedException e) {
                throw new AresException(e);
            }
        }
        return running || !handover.isEmpty();
    }

    protected void prepare() {
        if (!prepare) {
            return;
        }

        this.internalSource = createInternalSource();
        try {
            this.internalSource.open();
        } catch (Exception e) {
            running = false;
            throw new AresException("Failed to open internal source.", e);
        }

        this.internalRowCollector =
                new InternalRowCollector(
                        handover, checkpointLock, source.getProducedType(), envOptions);
        executorService.execute(
                () -> {
                    try {
                        internalSource.run(internalRowCollector);
                    } catch (Exception e) {
                        handover.reportError(e);
                        log.error("BatchPartitionReader execute failed.", e);
                        running = false;
                    }
                });
        prepare = false;
    }

    protected BaseSourceFunction<AresRow> createInternalSource() {
        return new InternalParallelSource<>(source, null, parallelism, subtaskId);
    }

    public InternalRow get() {
        try {
            return handover.pollNext().get();
        } catch (Exception e) {
            throw new AresException(e);
        }
    }

    public void close() throws IOException {
        running = false;
        try {
            if (internalSource != null) {
                internalSource.close();
            }
        } catch (Exception e) {
            throw new AresException(e);
        }
        executorService.shutdown();
    }

    public class InternalParallelSource<SplitT extends SourceSplit, StateT extends Serializable>
            extends ParallelSource<AresRow, SplitT, StateT> {

        public InternalParallelSource(
                AresSource<AresRow, SplitT, StateT> source,
                Map<Integer, List<byte[]>> restoredState,
                int parallelism,
                int subtaskId) {
            super(source, restoredState, parallelism, subtaskId);
        }

        @Override
        protected void handleNoMoreElement() {
            super.handleNoMoreElement();
            running = false;
        }
    }
}
