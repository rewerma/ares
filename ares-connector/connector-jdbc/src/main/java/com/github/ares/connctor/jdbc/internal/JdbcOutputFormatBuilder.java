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

    public JdbcOutputFormat build() {
        JdbcOutputFormat.StatementExecutorFactory statementExecutorFactory;

//        final String database = jdbcSinkConfig.getDatabase();
//        final String table = dialect.extractTableName(
//                TablePath.of(
//                        jdbcSinkConfig.getDatabase() + "." + jdbcSinkConfig.getTable()));

//        final List<String> primaryKeys = jdbcSinkConfig.getPrimaryKeys();
        if (StringUtils.isNotBlank(jdbcSinkConfig.getSimpleSql())) {
            statementExecutorFactory =
                    () ->
                            createSimpleBufferedExecutor(
                                    jdbcSinkConfig.getSimpleSql(),
                                    aresRowType,
                                    dialect.getRowConverter());
        } else if (StringUtils.isNotBlank(jdbcSinkConfig.getUpdateSql())) {
            statementExecutorFactory =
                    () ->
                            createSimpleBufferedExecutor(
                                    jdbcSinkConfig.getUpdateSql(),
                                    aresRowType,
                                    dialect.getRowConverter());
        } else if (StringUtils.isNotBlank(jdbcSinkConfig.getDeleteSql())) {
            statementExecutorFactory =
                    () ->
                            createSimpleBufferedExecutor(
                                    jdbcSinkConfig.getDeleteSql(),
                                    aresRowType,
                                    dialect.getRowConverter());
        } else {
            throw new AresException("unsupported operation");
        } /*else if (primaryKeys == null || primaryKeys.isEmpty()) {
            statementExecutorFactory =
                    () -> createSimpleBufferedExecutor(dialect, database, table, aresRowType);
        } else {
            statementExecutorFactory =
                    () ->
                            createUpsertBufferedExecutor(
                                    dialect,
                                    database,
                                    table,
                                    aresRowType,
                                    primaryKeys.toArray(new String[0]),
                                    jdbcSinkConfig.isEnableUpsert(),
                                    jdbcSinkConfig.isPrimaryKeyUpdated(),
                                    jdbcSinkConfig.isSupportUpsertByInsertOnly());
        }*/

        return new JdbcOutputFormat(
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

//    private static JdbcBatchStatementExecutor<AresRow> createUpsertBufferedExecutor(
//            JdbcDialect dialect,
//            String database,
//            String table,
//            AresRowType rowType,
//            String[] pkNames,
//            boolean enableUpsert,
//            boolean isPrimaryKeyUpdated,
//            boolean supportUpsertByInsertOnly) {
//        int[] pkFields = Arrays.stream(pkNames).mapToInt(rowType::indexOf).toArray();
//        AresDataType[] pkTypes =
//                Arrays.stream(pkFields)
//                        .mapToObj((IntFunction<AresDataType>) rowType::getFieldType)
//                        .toArray(AresDataType[]::new);
//
//        Function<AresRow, AresRow> keyExtractor = createKeyExtractor(pkFields);
//        JdbcBatchStatementExecutor<AresRow> deleteExecutor =
//                createDeleteExecutor(dialect, database, table, pkNames, pkTypes);
//        JdbcBatchStatementExecutor<AresRow> upsertExecutor =
//                createUpsertExecutor(
//                        dialect,
//                        database,
//                        table,
//                        rowType,
//                        pkNames,
//                        pkTypes,
//                        keyExtractor,
//                        enableUpsert,
//                        isPrimaryKeyUpdated,
//                        supportUpsertByInsertOnly);
//        return new BufferReducedBatchStatementExecutor(
//                upsertExecutor, deleteExecutor, keyExtractor, Function.identity());
//    }

//    private static JdbcBatchStatementExecutor<AresRow> createUpsertExecutor(
//            JdbcDialect dialect,
//            String database,
//            String table,
//            AresRowType rowType,
//            String[] pkNames,
//            AresDataType[] pkTypes,
//            Function<AresRow, AresRow> keyExtractor,
//            boolean enableUpsert,
//            boolean isPrimaryKeyUpdated,
//            boolean supportUpsertByInsertOnly) {
//        if (supportUpsertByInsertOnly) {
//            return createInsertOnlyExecutor(dialect, database, table, rowType);
//        }
//        if (enableUpsert) {
//            Optional<String> upsertSQL =
//                    dialect.getUpsertStatement(database, table, rowType.getFieldNames(), pkNames);
//            if (upsertSQL.isPresent()) {
//                return createSimpleExecutor(upsertSQL.get(), rowType, dialect.getRowConverter());
//            }
//            return createInsertOrUpdateByQueryExecutor(
//                    dialect,
//                    database,
//                    table,
//                    rowType,
//                    pkNames,
//                    pkTypes,
//                    keyExtractor,
//                    isPrimaryKeyUpdated);
//        }
//        return createInsertOrUpdateExecutor(
//                dialect, database, table, rowType, pkNames, isPrimaryKeyUpdated);
//    }

//    private static JdbcBatchStatementExecutor<AresRow> createInsertOnlyExecutor(
//            JdbcDialect dialect, String database, String table, AresRowType rowType) {
//
//        return new SimpleBatchStatementExecutor(
//                connection ->
//                        FieldNamedPreparedStatement.prepareStatement(
//                                connection,
//                                dialect.getInsertIntoStatement(
//                                        database, table, rowType.getFieldNames()),
//                                rowType.getFieldNames()),
//                rowType,
//                dialect.getRowConverter());
//    }
//
//    private static JdbcBatchStatementExecutor<AresRow> createInsertOrUpdateExecutor(
//            JdbcDialect dialect,
//            String database,
//            String table,
//            AresRowType rowType,
//            String[] pkNames,
//            boolean isPrimaryKeyUpdated) {
//
//        return new InsertOrUpdateBatchStatementExecutor(
//                connection ->
//                        FieldNamedPreparedStatement.prepareStatement(
//                                connection,
//                                dialect.getInsertIntoStatement(
//                                        database, table, rowType.getFieldNames()),
//                                rowType.getFieldNames()),
//                connection ->
//                        FieldNamedPreparedStatement.prepareStatement(
//                                connection,
//                                dialect.getUpdateStatement(
//                                        database,
//                                        table,
//                                        rowType.getFieldNames(),
//                                        pkNames,
//                                        isPrimaryKeyUpdated),
//                                rowType.getFieldNames()),
//                rowType,
//                dialect.getRowConverter());
//    }
//
//    private static JdbcBatchStatementExecutor<AresRow> createInsertOrUpdateByQueryExecutor(
//            JdbcDialect dialect,
//            String database,
//            String table,
//            AresRowType rowType,
//            String[] pkNames,
//            AresDataType[] pkTypes,
//            Function<AresRow, AresRow> keyExtractor,
//            boolean isPrimaryKeyUpdated) {
//        AresRowType keyRowType = new AresRowType(pkNames, pkTypes);
//        return new InsertOrUpdateBatchStatementExecutor(
//                connection ->
//                        FieldNamedPreparedStatement.prepareStatement(
//                                connection,
//                                dialect.getRowExistsStatement(database, table, pkNames),
//                                pkNames),
//                connection ->
//                        FieldNamedPreparedStatement.prepareStatement(
//                                connection,
//                                dialect.getInsertIntoStatement(
//                                        database, table, rowType.getFieldNames()),
//                                rowType.getFieldNames()),
//                connection ->
//                        FieldNamedPreparedStatement.prepareStatement(
//                                connection,
//                                dialect.getUpdateStatement(
//                                        database,
//                                        table,
//                                        rowType.getFieldNames(),
//                                        pkNames,
//                                        isPrimaryKeyUpdated),
//                                rowType.getFieldNames()),
//                keyRowType,
//                keyExtractor,
//                rowType,
//                dialect.getRowConverter());
//    }
//
//    private static JdbcBatchStatementExecutor<AresRow> createDeleteExecutor(
//            JdbcDialect dialect,
//            String database,
//            String table,
//            String[] pkNames,
//            AresDataType[] pkTypes) {
//        String deleteSQL = dialect.getDeleteStatement(database, table, pkNames);
//        return createSimpleExecutor(deleteSQL, pkNames, pkTypes, dialect.getRowConverter());
//    }

//    private static JdbcBatchStatementExecutor<AresRow> createSimpleExecutor(
//            String sql,
//            String[] fieldNames,
//            AresDataType[] fieldTypes,
//            JdbcRowConverter rowConverter) {
//        AresRowType rowType = new AresRowType(fieldNames, fieldTypes);
//        return createSimpleExecutor(sql, rowType, rowConverter);
//    }

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
