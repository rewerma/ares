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

package com.github.ares.connctor.jdbc.internal;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.config.JdbcSinkConfig;
import com.github.ares.connctor.jdbc.internal.connection.JdbcConnectionProvider;
import com.github.ares.connctor.jdbc.internal.converter.JdbcRowConverter;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.executor.BufferReducedBatchStatementExecutor;
import com.github.ares.connctor.jdbc.internal.executor.BufferedBatchStatementExecutor;
import com.github.ares.connctor.jdbc.internal.executor.FieldNamedPreparedStatement;
import com.github.ares.connctor.jdbc.internal.executor.InsertOrUpdateBatchStatementExecutor;
import com.github.ares.connctor.jdbc.internal.executor.JdbcBatchStatementExecutor;
import com.github.ares.connctor.jdbc.internal.executor.SimpleBatchStatementExecutor;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;

public class JdbcOutputFormatBuilder {
    private final JdbcDialect dialect;
    private final JdbcConnectionProvider connectionProvider;
    private final JdbcSinkConfig jdbcSinkConfig;
    private final AresRowType aresRowType;

    public JdbcOutputFormatBuilder(JdbcDialect dialect, JdbcConnectionProvider connectionProvider, JdbcSinkConfig jdbcSinkConfig, AresRowType aresRowType) {
        this.dialect = dialect;
        this.connectionProvider = connectionProvider;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.aresRowType = aresRowType;
    }

    public JdbcOutputFormat<AresRow, JdbcBatchStatementExecutor<AresRow>> build() {
        JdbcOutputFormat.StatementExecutorFactory<JdbcBatchStatementExecutor<AresRow>> statementExecutorFactory;

        if (StringUtils.isNotBlank(jdbcSinkConfig.getSimpleSql())) {
            statementExecutorFactory =
                    () ->
                            createSimpleBufferedExecutor(
                                    jdbcSinkConfig.getSimpleSql(),
                                    aresRowType,
                                    dialect.getRowConverter());
        } else {
            throw new AresException("unsupported operation");
        }

        return new JdbcOutputFormat<>(
                connectionProvider,
                jdbcSinkConfig.getJdbcConnectionConfig(),
                statementExecutorFactory);
    }

    private static JdbcBatchStatementExecutor<AresRow> createSimpleBufferedExecutor(
            JdbcDialect dialect, String database, String table, AresRowType rowType) {
        String insertSQL = dialect.getInsertIntoStatement(database, table, rowType.getFieldNames());
        return createSimpleBufferedExecutor(insertSQL, rowType, dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<AresRow> createSimpleBufferedExecutor(
            String sql, AresRowType rowType, JdbcRowConverter rowConverter) {
        JdbcBatchStatementExecutor<AresRow> simpleRowExecutor =
                createSimpleExecutor(sql, rowType, rowConverter);
        return new BufferedBatchStatementExecutor(simpleRowExecutor, Function.identity());
    }

    private static JdbcBatchStatementExecutor<AresRow> createSimpleExecutor(
            String sql, AresRowType rowType, JdbcRowConverter rowConverter) {
        return new SimpleBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, sql, rowType.getFieldNames()),
                rowType,
                rowConverter);
    }

    static Function<AresRow, AresRow> createKeyExtractor(int[] pkFields) {
        return row -> {
            Object[] fields = new Object[pkFields.length];
            for (int i = 0; i < pkFields.length; i++) {
                fields[i] = row.getField(pkFields[i]);
            }
            AresRow newRow = new AresRow(fields);
            newRow.setTableId(row.getTableId());
            return newRow;
        };
    }
}
