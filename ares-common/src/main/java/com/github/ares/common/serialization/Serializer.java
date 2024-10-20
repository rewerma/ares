package com.github.ares.common.serialization;

import java.io.IOException;

public interface Serializer<T> {

    /**
     * Serializes the given object.
     *
     * @param obj The object to serialize.
     * @return The serialized data (bytes).
     * @throws IOException Thrown, if the serialization fails.
     */
    byte[] serialize(T obj) throws IOException;

    /**
     * De-serializes the given data (bytes).
     *
     * @param serialized The serialized data
     * @return The deserialized object
     * @throws IOException Thrown, if the deserialization fails.
     */
    T deserialize(byte[] serialized) throws IOException;
}
