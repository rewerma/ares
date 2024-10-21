package com.github.ares.api.source;

import com.github.ares.api.table.event.SchemaChangeEvent;
import com.github.ares.api.table.type.AresDataType;

public interface Collector<T> {

    void collect(T record);

    void resetRowType(AresDataType<?> dataType);

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
