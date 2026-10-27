package com.github.ares.engine.spark.core;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connector.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connector.jdbc.sink.JdbcPlTransactionSession;
import com.github.ares.connector.jdbc.sink.JdbcSink;
import java.util.Iterator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

/**
 * JDBC {@code INSERT OVERWRITE}. The query is materialized first, then existing rows are removed
 * and the new rows are inserted on one {@code autoCommit=false} connection.
 */
final class JdbcOverwriteWriter {
    private JdbcOverwriteWriter() {}

    static void execute(
            SparkExecutorManager manager,
            JdbcSink jdbcSink,
            Dataset<Row> data,
            CatalogTable catalogTable,
            String sinkTableName) {
        rejectAnalyticsDialect(jdbcSink);
        // Cache before the clear. Spark recomputes each action, and the query may read this table.
        Dataset<Row> cached = data.persist(StorageLevel.MEMORY_AND_DISK());
        try {
            cached.count();
            if (manager.getTransactionManager().isActive()) {
                manager.prepareJdbcOverwrite(jdbcSink, sinkTableName);
                if (!manager.tryTransactionalSink(jdbcSink, cached, catalogTable, sinkTableName)) {
                    throw new AresException("INSERT OVERWRITE lost the active JDBC transaction");
                }
                return;
            }
            writeAlone(jdbcSink, cached, catalogTable);
        } finally {
            cached.unpersist();
        }
    }

    private static void writeAlone(
            JdbcSink jdbcSink, Dataset<Row> cached, CatalogTable catalogTable) {
        JdbcPlTransactionSession session = new JdbcPlTransactionSession();
        try {
            session.clearForOverwrite(jdbcSink);
            AresRowType rowType = jdbcSink.getAresRowType();
            if (rowType == null) {
                rowType = catalogTable.getAresRowType();
            }
            Iterator<Row> rows = cached.toLocalIterator();
            while (rows.hasNext()) {
                session.write(
                        jdbcSink,
                        SparkJdbcTransactionalSinkHandler.toAresRow(rows.next(), rowType));
            }
            session.commit();
        } catch (AresException e) {
            session.rollbackQuietly();
            throw e;
        } catch (Exception e) {
            session.rollbackQuietly();
            throw new AresException("INSERT OVERWRITE failed: " + e.getMessage(), e);
        } finally {
            session.close();
        }
    }

    private static void rejectAnalyticsDialect(JdbcSink jdbcSink) {
        String dialect = jdbcSink.dialect().dialectName();
        if (DatabaseIdentifier.DORIS.equalsIgnoreCase(dialect)
                || DatabaseIdentifier.STARROCKS.equalsIgnoreCase(dialect)) {
            throw new AresException(
                    "INSERT OVERWRITE for "
                            + dialect
                            + " truncates the table and then inserts. Use the Doris or StarRocks connector.");
        }
    }
}
