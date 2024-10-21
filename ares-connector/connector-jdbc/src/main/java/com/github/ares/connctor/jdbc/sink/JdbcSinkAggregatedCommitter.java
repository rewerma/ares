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

import com.github.ares.api.sink.SinkAggregatedCommitter;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.config.JdbcSinkConfig;
import com.github.ares.connctor.jdbc.internal.xa.GroupXaOperationResult;
import com.github.ares.connctor.jdbc.internal.xa.XaFacade;
import com.github.ares.connctor.jdbc.internal.xa.XaGroupOps;
import com.github.ares.connctor.jdbc.internal.xa.XaGroupOpsImpl;
import com.github.ares.connctor.jdbc.state.JdbcAggregatedCommitInfo;
import com.github.ares.connctor.jdbc.state.XidInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JdbcSinkAggregatedCommitter
        implements SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo> {
    private static final Logger log = LoggerFactory.getLogger(JdbcSinkAggregatedCommitter.class);

    private final XaFacade xaFacade;
    private final XaGroupOps xaGroupOps;
    private final JdbcSinkConfig jdbcSinkConfig;

    public JdbcSinkAggregatedCommitter(JdbcSinkConfig jdbcSinkConfig) {
        this.xaFacade =
                XaFacade.fromJdbcConnectionOptions(jdbcSinkConfig.getJdbcConnectionConfig());
        this.xaGroupOps = new XaGroupOpsImpl(xaFacade);
        this.jdbcSinkConfig = jdbcSinkConfig;
    }

    private void tryOpen() throws IOException {
        if (!xaFacade.isOpen()) {
            try {
                xaFacade.open();
            } catch (Exception e) {
                throw new AresException(
                        "unable to open JDBC sink aggregated committer",
                        e);
            }
        }
    }

    @Override
    public List<JdbcAggregatedCommitInfo> commit(
            List<JdbcAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        tryOpen();
        return aggregatedCommitInfos.stream()
                .map(
                        aggregatedCommitInfo -> {
                            log.info("commit xid: " + aggregatedCommitInfo.getXidInfoList());
                            GroupXaOperationResult<XidInfo> result =
                                    xaGroupOps.commit(
                                            new ArrayList<>(aggregatedCommitInfo.getXidInfoList()),
                                            false,
                                            jdbcSinkConfig
                                                    .getJdbcConnectionConfig()
                                                    .getMaxCommitAttempts());
                            return new JdbcAggregatedCommitInfo(result.getForRetry());
                        })
                .filter(ainfo -> !ainfo.getXidInfoList().isEmpty())
                .collect(Collectors.toList());
    }

    @Override
    public JdbcAggregatedCommitInfo combine(List<XidInfo> commitInfos) {
        return new JdbcAggregatedCommitInfo(commitInfos);
    }

    @Override
    public void abort(List<JdbcAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        tryOpen();
        for (JdbcAggregatedCommitInfo commitInfos : aggregatedCommitInfo) {
            xaGroupOps.rollback(commitInfos.getXidInfoList());
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (xaFacade.isOpen()) {
                xaFacade.close();
            }
        } catch (Exception e) {
            throw new AresException(
                    "unable to close JDBC sink aggregated committer",
                    e);
        }
    }
}