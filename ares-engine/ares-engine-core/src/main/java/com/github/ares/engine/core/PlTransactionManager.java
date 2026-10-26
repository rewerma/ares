package com.github.ares.engine.core;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.common.exceptions.AresException;
import java.io.Serializable;

/**
 * PL transaction segment: {@code START TRANSACTION} ... {@code END TRANSACTION}. JDBC DML inside
 * the segment is batched on a Driver-held connection. {@code COMMIT} saves the current batch and
 * keeps the segment open, so later DML starts another batch without a new {@code START
 * TRANSACTION}. {@code END TRANSACTION} commits a still-open batch and leaves the segment. {@code
 * ROLLBACK} undoes the current batch and keeps the segment open. Each of these statements is a
 * no-op when it does not apply. Block-end {@link #close()} rolls back only a still-open batch.
 */
public class PlTransactionManager implements Serializable {
    private static final long serialVersionUID = -1L;

    /** True from {@code START TRANSACTION} until {@code END TRANSACTION} or {@link #close()}. */
    private transient boolean segment;

    /**
     * True when the current batch has not been committed or rolled back. {@code START TRANSACTION}
     * opens a batch immediately, and the next DML after {@code COMMIT} or {@code ROLLBACK} opens
     * another one.
     */
    private transient boolean unitOpen;

    private transient TransactionalSinkHandler handler;

    public void init(TransactionalSinkHandler handler) {
        this.handler = handler;
    }

    public boolean isActive() {
        return segment;
    }

    public void start() {
        if (segment) {
            return;
        }
        segment = true;
        unitOpen = true;
    }

    public void commit() {
        if (!segment || !unitOpen) {
            return;
        }
        if (handler != null) {
            handler.commit();
        }
        unitOpen = false;
    }

    /**
     * Explicit {@code ROLLBACK}. No-op when the current batch is already closed, so an EXCEPTION
     * handler can still issue ROLLBACK after the engine has already rolled back.
     */
    public void rollback() {
        if (!segment || !unitOpen) {
            return;
        }
        rollbackQuietly();
    }

    public void rollbackQuietly() {
        try {
            if (handler != null) {
                handler.rollbackQuietly();
            }
        } finally {
            unitOpen = false;
        }
    }

    /**
     * Leave the segment. Commits the current batch when one is still open, so a partial batch at
     * the end of the segment is saved.
     */
    public void end() {
        if (!segment) {
            return;
        }
        if (unitOpen) {
            if (handler != null) {
                handler.commit();
            }
            unitOpen = false;
        }
        segment = false;
    }

    /**
     * Release the transactional connection. Rolls back only if a batch is still open (block ended
     * without COMMIT or END TRANSACTION). Already-committed work is kept.
     */
    public void close() {
        try {
            if (unitOpen) {
                rollbackQuietly();
            }
            if (handler != null) {
                handler.close();
            }
        } finally {
            segment = false;
            unitOpen = false;
        }
    }

    public void write(
            AresSink<?, ?, ?, ?> sink,
            Object dataset,
            CatalogTable catalogTable,
            String sinkTableName) {
        ensureActive("DML");
        unitOpen = true;
        if (handler == null) {
            throw new AresException("Transactional sink handler is not initialized");
        }
        handler.write(sink, dataset, catalogTable, sinkTableName);
    }

    public void ensureNotActive(String operation) {
        if (segment) {
            throw new AresException(operation + " is not supported inside START TRANSACTION");
        }
    }

    private void ensureActive(String operation) {
        if (!segment) {
            throw new AresException(operation + " requires START TRANSACTION");
        }
    }
}
