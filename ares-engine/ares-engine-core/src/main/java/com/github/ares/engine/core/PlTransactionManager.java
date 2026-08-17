package com.github.ares.engine.core;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.common.exceptions.AresException;

import java.io.Serializable;

/**
 * PL transaction mode: {@code START TRANSACTION} / {@code COMMIT} / {@code ROLLBACK}.
 * JDBC DML inside the mode is batched on a Driver-held connection until COMMIT.
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
            throw new AresException("Transaction already started");
        }
        active = true;
    }

    public void commit() {
        ensureActive("COMMIT");
        if (handler != null) {
            handler.commit();
        }
    }

    public void rollback() {
        ensureActive("ROLLBACK");
        rollbackQuietly();
    }

    public void rollbackQuietly() {
        if (handler != null) {
            handler.rollbackQuietly();
        }
    }

    public void close() {
        try {
            if (handler != null) {
                handler.close();
            }
        } finally {
            active = false;
        }
    }

    public void write(AresSink<?, ?, ?, ?> sink, Object dataset, CatalogTable catalogTable, String sinkTableName) {
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
