package com.github.ares.engine.spark.core;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connector.jdbc.sink.JdbcPlTransactionSession;
import com.github.ares.connector.jdbc.sink.JdbcSink;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.TransactionalSinkHandler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

public class SparkJdbcTransactionalSinkHandler implements TransactionalSinkHandler {

    private final ExecutorManager executorManager;
    private final JdbcPlTransactionSession session = new JdbcPlTransactionSession();
    private final Set<String> dirtyTables = new LinkedHashSet<>();

    public SparkJdbcTransactionalSinkHandler(ExecutorManager executorManager) {
        this.executorManager = executorManager;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(AresSink<?, ?, ?, ?> sink, Object dataset, CatalogTable catalogTable, String sinkTableName) {
        if (!(sink instanceof JdbcSink)) {
            throw new AresException(
                    "START TRANSACTION only supports JDBC sink, got: " + sink.getPluginName());
        }
        if (sinkTableName != null) {
            dirtyTables.add(sinkTableName);
        }
        JdbcSink jdbcSink = (JdbcSink) sink;
        Dataset<Row> resultDf = (Dataset<Row>) dataset;
        Dataset<Row> cached = resultDf.persist(StorageLevel.MEMORY_AND_DISK());
        try {
            AresRowType rowType = jdbcSink.getAresRowType();
            if (rowType == null) {
                rowType = catalogTable.getAresRowType();
            }
            Iterator<Row> rows = cached.toLocalIterator();
            while (rows.hasNext()) {
                session.write(jdbcSink, toAresRow(rows.next(), rowType));
            }
        } finally {
            cached.unpersist();
        }
    }

    @Override
    public void commit() {
        session.commit();
        for (String tableName : dirtyTables) {
            if (executorManager.getSourceTables().containsKey(tableName.toLowerCase(Locale.ROOT))) {
                executorManager.getReloadFunctionExecutor().reloadSourceTable(tableName);
            }
        }
    }

    @Override
    public void rollbackQuietly() {
        session.rollbackQuietly();
    }

    @Override
    public void close() {
        session.close();
        dirtyTables.clear();
    }

    static AresRow toAresRow(Row row, AresRowType rowType) {
        int arity = rowType.getTotalFields();
        Object[] fields = new Object[arity];
        for (int i = 0; i < arity; i++) {
            fields[i] = convertField(row.get(i), rowType.getFieldType(i));
        }
        return new AresRow(fields);
    }

    private static Object convertField(Object value, AresDataType<?> dataType) {
        if (value == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case STRING:
                return value.toString();
            case DATE:
                if (value instanceof Date) {
                    return ((Date) value).toLocalDate();
                }
                if (value instanceof LocalDate) {
                    return value;
                }
                break;
            case TIMESTAMP:
                if (value instanceof Timestamp) {
                    return ((Timestamp) value).toLocalDateTime();
                }
                if (value instanceof LocalDateTime) {
                    return value;
                }
                break;
            case DECIMAL:
                if (value instanceof BigDecimal) {
                    return value;
                }
                if (value instanceof Number) {
                    return new BigDecimal(value.toString());
                }
                break;
            default:
                break;
        }
        return value;
    }
}
