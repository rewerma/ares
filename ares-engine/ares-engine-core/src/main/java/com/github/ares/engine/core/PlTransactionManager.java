package com.github.ares.engine.core;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.common.exceptions.AresException;
import java.io.Serializable;

/**
 * PL transaction mode: {@code START TRANSACTION} / {@code COMMIT} / {@code ROLLBACK}. JDBC DML
 * inside the mode is batched on a Driver-held connection until COMMIT. {@code START TRANSACTION} is
 * idempotent (no-op if already active) so it can sit at the head of a loop. {@code COMMIT} is a
 * no-op when no transaction is active. Block-end {@link #close()} only rolls back if a transaction
 * is still active.
 */
public class PlTransactionManager implements Serializable {
    private static final long serialVersionUID = -1L;

    private transient boolean active;
    private transient TransactionalSinkHandler handler;

    public void init(TransactionalSinkHandler handler) {
        this.handler = handler;
    }

    public boolean isActive() {
        return active;
    }

    public void start() {
        if (active) {
            return;
        }
        active = true;
    }

    public void commit() {
        if (!active) {
            return;
        }
        if (handler != null) {
            handler.commit();
        }
        active = false;
    }

    /**
     * Explicit {@code ROLLBACK}. No-op when no transaction is active, so an EXCEPTION handler can
     * still issue ROLLBACK after the engine has already rolled back.
     */
    public void rollback() {
        if (!active) {
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
            active = false;
        }
    }

    /**
     * Release the transactional connection. Rolls back only if a transaction is still active (block
     * ended without COMMIT). Already-committed work is kept.
     */
    public void close() {
        try {
            if (active) {
                rollbackQuietly();
            }
            if (handler != null) {
                handler.close();
            }
        } finally {
            active = false;
        }
    }

    public void write(
            AresSink<?, ?, ?, ?> sink,
            Object dataset,
            CatalogTable catalogTable,
            String sinkTableName) {
        ensureActive("DML");
        if (handler == null) {
            throw new AresException("Transactional sink handler is not initialized");
        }
        handler.write(sink, dataset, catalogTable, sinkTableName);
    }

    public void ensureNotActive(String operation) {
        if (active) {
            throw new AresException(operation + " is not supported inside START TRANSACTION");
        }
    }

    private void ensureActive(String operation) {
        if (!active) {
            throw new AresException(operation + " requires START TRANSACTION");
        }
    }
}
