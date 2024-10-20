package com.github.ares.api.table.type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MapType<K, V> implements CompositeType<Map<K, V>> {

    private static final List<SqlType> SUPPORTED_KEY_TYPES =
            Arrays.asList(
                    SqlType.NULL,
                    SqlType.BOOLEAN,
                    SqlType.TINYINT,
                    SqlType.SMALLINT,
                    SqlType.INT,
                    SqlType.BIGINT,
                    SqlType.DATE,
                    SqlType.TIME,
                    SqlType.TIMESTAMP,
                    SqlType.FLOAT,
                    SqlType.DOUBLE,
                    SqlType.STRING,
                    SqlType.DECIMAL);

    private final AresDataType<K> keyType;
    private final AresDataType<V> valueType;

    public MapType(AresDataType<K> keyType, AresDataType<V> valueType) {
        if(keyType==null){
            throw new RuntimeException("The key type is required.");
        }
        if(valueType==null){
            throw new RuntimeException("The value type is required.");
        }
        if(!SUPPORTED_KEY_TYPES.contains(keyType.getSqlType())){
            throw new RuntimeException(String.format("Unsupported key types: %s",keyType));
        }
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public AresDataType<K> getKeyType() {
        return keyType;
    }

    public AresDataType<V> getValueType() {
        return valueType;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Map<K, V>> getTypeClass() {
        return (Class<Map<K, V>>) (Class<?>) Map.class;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.MAP;
    }

    @Override
    public List<AresDataType<?>> getChildren() {
        List<AresDataType<?>> result = new ArrayList<>();
        result.add(keyType);
        result.add(valueType);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof MapType)) {
            return false;
        }
        MapType<?, ?> that = (MapType<?, ?>) obj;
        return Objects.equals(keyType, that.keyType) && Objects.equals(valueType, that.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyType, valueType);
    }

    @Override
    public String toString() {
        return String.format("Map<%s, %s>", keyType, valueType);
    }
}