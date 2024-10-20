package com.github.ares.common.serialization;

import com.github.ares.common.utils.SerializationUtils;

import java.io.IOException;
import java.io.Serializable;

public class DefaultSerializer<T extends Serializable> implements Serializer<T> {

    @Override
    public byte[] serialize(T obj) throws IOException {
        if (obj != null) {
            return SerializationUtils.serialize((Serializable) obj);
        } else {
            return null;
        }
    }

    @Override
    public T deserialize(byte[] serialized) throws IOException {
        if (serialized == null) {
            return null;
        }
        return SerializationUtils.deserialize(serialized);
    }
}
