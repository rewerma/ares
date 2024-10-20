package com.github.ares.api.serialization;

import com.github.ares.api.table.type.AresDataType;

import java.io.IOException;
import java.io.Serializable;

public interface DeserializationSchema<T> extends Serializable {

    /**
     * Deserializes the byte message.
     *
     * @param message The message, as a byte array.
     * @return The deserialized message as an Ares Row (null if the message cannot be
     *     deserialized).
     */
    T deserialize(byte[] message) throws IOException;

//    default void deserialize(byte[] message, Collector<T> out) throws IOException {
//        T deserialize = deserialize(message);
//        if (deserialize != null) {
//            out.collect(deserialize);
//        }
//    }

    AresDataType<T> getProducedType();
}
