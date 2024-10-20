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

package com.github.ares.connctor.jdbc.sink;

import com.github.ares.api.common.JobContext;
import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.config.JdbcSinkConfig;
import com.github.ares.connctor.jdbc.internal.JdbcOutputFormat;
import com.github.ares.connctor.jdbc.internal.JdbcOutputFormatBuilder;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.executor.JdbcBatchStatementExecutor;
import com.github.ares.connctor.jdbc.internal.xa.XaFacade;
import com.github.ares.connctor.jdbc.internal.xa.XaGroupOps;
import com.github.ares.connctor.jdbc.internal.xa.XaGroupOpsImpl;
import com.github.ares.connctor.jdbc.internal.xa.XidGenerator;
import com.github.ares.connctor.jdbc.state.JdbcSinkState;
import com.github.ares.connctor.jdbc.state.XidInfo;
import com.google.common.base.Throwables;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.github.ares.com.google.common.base.Preconditions.checkArgument;
import static com.github.ares.com.google.common.base.Preconditions.checkState;

public class JdbcExactlyOnceSinkWriter
        implements SinkWriter<AresRow, XidInfo, JdbcSinkState>,
        SupportMultiTableSinkWriter<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcExactlyOnceSinkWriter.class);

    private final SinkWriter.Context sinkcontext;

    private final JobContext context;

    private final List<JdbcSinkState> recoverStates;

    private final XaFacade xaFacade;

    private final XaGroupOps xaGroupOps;

    private final XidGenerator xidGenerator;

    private final JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>>
            outputFormat;

    private transient boolean isOpen;

    private transient Xid currentXid;
    private transient Xid prepareXid;

    public JdbcExactlyOnceSinkWriter(
            SinkWriter.Context sinkcontext,
            JobContext context,
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            AresRowType rowType,
            List<JdbcSinkState> states) {
        checkArgument(
                jdbcSinkConfig.getJdbcConnectionConfig().getMaxRetries() == 0,
                "JDBC XA sink requires maxRetries equal to 0, otherwise it could "
                        + "cause duplicates.");

        this.context = context;
        this.sinkcontext = sinkcontext;
        this.recoverStates = states;
        this.xidGenerator = XidGenerator.semanticXidGenerator();
        checkState(jdbcSinkConfig.isExactlyOnce(), "is_exactly_once config error");
        this.xaFacade =
                XaFacade.fromJdbcConnectionOptions(jdbcSinkConfig.getJdbcConnectionConfig());
        this.outputFormat =
                new JdbcOutputFormatBuilder(dialect, xaFacade, jdbcSinkConfig, rowType).build();
        this.xaGroupOps = new XaGroupOpsImpl(xaFacade);
    }

    private void tryOpen() {
        if (!isOpen) {
            isOpen = true;
            try {
                xidGenerator.open();
                xaFacade.open();
                outputFormat.open();
                if (!recoverStates.isEmpty()) {
                    Xid xid = recoverStates.get(0).getXid();
                    // Rollback pending transactions that should not include recoverStates
                    xaGroupOps.recoverAndRollback(context, sinkcontext, xidGenerator, xid);
                }
                beginTx();
            } catch (Exception e) {
                throw new AresException(
                        "unable to open JDBC exactly one writer",
                        e);
            }
        }
    }

    @Override
    public List<JdbcSinkState> snapshotState(long checkpointId) {
        checkState(prepareXid != null, "prepare xid must not be null");
        return Collections.singletonList(new JdbcSinkState(prepareXid));
    }

    @Override
    public void write(AresRow element) {
        tryOpen();
        checkState(currentXid != null, "current xid must not be null");
        AresRow copy = SerializationUtils.clone(element);
        outputFormat.writeRecord(copy);
    }

    @Override
    public Optional<XidInfo> prepareCommit() throws IOException {
        tryOpen();

        boolean emptyXaTransaction = false;
        try {
            prepareCurrentTx();
        } catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof XaFacade.EmptyXaTransactionException) {
                emptyXaTransaction = true;
                LOG.info("skip prepare empty xa transaction, xid={}", currentXid);
            } else {
                throw e;
            }
        }
        this.currentXid = null;
        beginTx();
        checkState(prepareXid != null, "prepare xid must not be null");
        return emptyXaTransaction ? Optional.empty() : Optional.of(new XidInfo(prepareXid, 0));
    }

    @Override
    public void abortPrepare() {
    }

    @Override
    public void close() throws IOException {
        if (currentXid != null && xaFacade.isOpen()) {
            try {
                LOG.debug("remove current transaction before closing, xid={}", currentXid);
                xaFacade.failAndRollback(currentXid);
            } catch (Exception e) {
                LOG.warn("unable to fail/rollback current transaction, xid={}", currentXid, e);
            }
        }
        try {
            xaFacade.close();
        } catch (Exception e) {
            throw new AresException(
                    "unable to close JDBC exactly one writer",
                    e);
        }
        xidGenerator.close();
        currentXid = null;
        prepareXid = null;
    }

    private void beginTx() throws IOException {
        checkState(currentXid == null, "currentXid not null");
        currentXid = xidGenerator.generateXid(context, sinkcontext, System.currentTimeMillis());
        try {
            xaFacade.start(currentXid);
        } catch (Exception e) {
            throw new AresException(
                    "unable to start xa transaction",
                    e);
        }
    }

    private void prepareCurrentTx() throws IOException {
        checkState(currentXid != null, "no current xid");
        outputFormat.flush();

        Exception endAndPrepareException = null;
        try {
            xaFacade.endAndPrepare(currentXid);
        } catch (Exception e) {
            endAndPrepareException = e;
            throw new AresException(
                    "unable to prepare current xa transaction", e);
        } finally {
            if (endAndPrepareException == null
                    || Throwables.getRootCause(endAndPrepareException)
                    instanceof XaFacade.EmptyXaTransactionException) {
                prepareXid = currentXid;
            }
        }
    }
}
