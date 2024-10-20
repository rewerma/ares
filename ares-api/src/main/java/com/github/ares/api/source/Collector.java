package com.github.ares.api.source;

import com.github.ares.api.table.event.SchemaChangeEvent;

public interface Collector<T> {

    void collect(T record);

    default void markSchemaChangeBeforeCheckpoint() {
    }

    default void collect(SchemaChangeEvent event) {
    }

    default void markSchemaChangeAfterCheckpoint() {
    }

    /**
     * Returns the checkpoint lock.
     *
     * @return The object to use as the lock
     */
    Object getCheckpointLock();

    default boolean isEmptyThisPollNext() {
        return false;
    }

    default void resetEmptyThisPollNext() {
    }
}
