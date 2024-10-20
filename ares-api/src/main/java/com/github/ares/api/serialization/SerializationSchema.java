package com.github.ares.api.serialization;

import com.github.ares.api.table.type.AresRow;

import java.io.Serializable;

public interface SerializationSchema extends Serializable {
    /**
     * Serializes the incoming element to a specified type.
     *
     * @param element The incoming element to be serialized
     * @return The serialized element.
     */
    byte[] serialize(AresRow element);
}
