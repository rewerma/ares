package com.github.ares.engine.core;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;

/** Engine-specific writer used while a PL transaction is active. */
public interface TransactionalSinkHandler {

    void write(
            AresSink<?, ?, ?, ?> sink,
            Object dataset,
            CatalogTable catalogTable,
            String sinkTableName);

    /**
     * Clears a JDBC target before {@link #write} on the same open transaction. The manager commits
     * or rolls back both steps together.
     */
    void prepareOverwrite(AresSink<?, ?, ?, ?> sink, String sinkTableName);

    void commit();

    void rollbackQuietly();

    /**
     * Release writers and the JDBC connection. Does not roll back; the manager rolls back leftover
     * work before calling this when a transaction is still active.
     */
    void close();
}
