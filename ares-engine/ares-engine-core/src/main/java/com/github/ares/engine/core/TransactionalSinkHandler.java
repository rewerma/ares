package com.github.ares.engine.core;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;

/**
 * Engine-specific writer used while a PL transaction is active.
 */
public interface TransactionalSinkHandler {

    void write(AresSink<?, ?, ?, ?> sink, Object dataset, CatalogTable catalogTable, String sinkTableName);

    void commit();

    void rollbackQuietly();

    void close();
}
