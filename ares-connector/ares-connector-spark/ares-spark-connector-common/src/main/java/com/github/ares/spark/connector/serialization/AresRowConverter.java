package com.github.ares.spark.connector.serialization;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.MapType;
import com.github.ares.connector.serialization.RowConverter;
import com.github.ares.spark.connector.utils.TypeConverterUtils;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Tuple2;
import scala.collection.immutable.HashMap.HashTrieMap;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class AresRowConverter extends RowConverter<AresRow> {
    public AresRowConverter(AresDataType<?> dataType) {
        super(dataType);
    }

    // AresRow To GenericRow
    @Override
    public AresRow convert(AresRow aresRow) throws IOException {
        validate(aresRow);
        GenericRowWithSchema rowWithSchema = (GenericRowWithSchema) convert(aresRow, dataType);
        AresRow newRow = new AresRow(rowWithSchema.values());
        newRow.setRowKind(aresRow.getRowKind());
        newRow.setTableId(aresRow.getTableId());
        return newRow;
    }

    private Object convert(Object field, AresDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case ROW:
                AresRow aresRow = (AresRow) field;
                AresRowType rowType = (AresRowType) dataType;
                return convertRow(aresRow, rowType);
            case DATE:
                return Date.valueOf((LocalDate) field);
            case TIMESTAMP:
                return Timestamp.valueOf((LocalDateTime) field);
            case TIME:
                if (field instanceof LocalTime) {
                    return ((LocalTime) field).toNanoOfDay();
                }
                if (field instanceof Long) {
                    return field;
                }
            case STRING:
                return field.toString();
            case MAP:
                return convertMap((Map<?, ?>) field, (MapType<?, ?>) dataType);
            case ARRAY:
                // if string array, we need to covert every item in array from String to UTF8String
                if (((ArrayType<?, ?>) dataType).getElementType().equals(BasicType.STRING_TYPE)) {
                    Object[] fields = (Object[]) field;
                    Object[] objects =
                            Arrays.stream(fields)
                                    .map(v -> UTF8String.fromString((String) v))
                                    .toArray();
                    return convertArray(objects, (ArrayType<?, ?>) dataType);
                }
                // except string, now only support convert boolean int tinyint smallint bigint float
                // double, because Ares Array only support these types
                return convertArray((Object[]) field, (ArrayType<?, ?>) dataType);
            default:
                if (field instanceof scala.Some) {
                    return ((scala.Some<?>) field).get();
                }
                return field;
        }
    }

    private GenericRowWithSchema convertRow(AresRow AresRow, AresRowType rowType) {
        int arity = rowType.getTotalFields();
        Object[] values = new Object[arity];
        StructType schema = (StructType) TypeConverterUtils.convert(rowType);
        for (int i = 0; i < arity; i++) {
            Object fieldValue = convert(AresRow.getField(i), rowType.getFieldType(i));
            if (fieldValue != null) {
                values[i] = fieldValue;
            }
        }
        return new GenericRowWithSchema(values, schema);
    }

    private scala.collection.immutable.HashMap<Object, Object> convertMap(
            Map<?, ?> mapData, MapType<?, ?> mapType) {
        scala.collection.immutable.HashMap<Object, Object> newMap =
                new scala.collection.immutable.HashMap<>();
        if (mapData.isEmpty()) {
            return newMap;
        }
        int num = mapData.size();
        Object[] keys = mapData.keySet().toArray();
        Object[] values = mapData.values().toArray();
        for (int i = 0; i < num; i++) {
            keys[i] = convert(keys[i], mapType.getKeyType());
            values[i] = convert(values[i], mapType.getValueType());
            Tuple2<Object, Object> tuple2 = new Tuple2<>(keys[i], values[i]);
            newMap = newMap.$plus(tuple2);
        }

        return newMap;
    }

    private WrappedArray.ofRef<?> convertArray(Object[] arrayData, ArrayType<?, ?> arrayType) {
        if (arrayData.length == 0) {
            return new WrappedArray.ofRef<>(new Object[0]);
        }
        int num = arrayData.length;
        for (int i = 0; i < num; i++) {
            arrayData[i] = convert(arrayData[i], arrayType.getElementType());
        }
        return new WrappedArray.ofRef<>(arrayData);
    }

    // GenericRow To Ares
    @Override
    public AresRow reconvert(AresRow engineRow) throws IOException {
        return (AresRow) reconvert(engineRow, dataType);
    }

    private Object reconvert(Object field, AresDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case ROW:
                if (field instanceof GenericRowWithSchema) {
                    return createFromGenericRow(
                            (GenericRowWithSchema) field, (AresRowType) dataType);
                }
                return reconvert((AresRow) field, (AresRowType) dataType);
            case DATE:
                return ((Date) field).toLocalDate();
            case TIMESTAMP:
                return ((Timestamp) field).toLocalDateTime();
            case TIME:
                if (field instanceof Timestamp) {
                    return ((Timestamp) field).toLocalDateTime().toLocalTime();
                }
                return LocalTime.ofNanoOfDay((Long) field);
            case STRING:
                return field.toString();
            case MAP:
                return reconvertMap((HashTrieMap<?, ?>) field, (MapType<?, ?>) dataType);
            case ARRAY:
                return reconvertArray((WrappedArray.ofRef<?>) field, (ArrayType<?, ?>) dataType);
            default:
                return field;
        }
    }

    private AresRow createFromGenericRow(GenericRowWithSchema row, AresRowType type) {
        Object[] fields = row.values();
        Object[] newFields = new Object[fields.length];
        for (int idx = 0; idx < fields.length; idx++) {
            newFields[idx] = reconvert(fields[idx], type.getFieldType(idx));
        }
        return new AresRow(newFields);
    }

    private AresRow reconvert(AresRow engineRow, AresRowType rowType) {
        int num = engineRow.getFields().length;
        Object[] fields = new Object[num];
        for (int i = 0; i < num; i++) {
            fields[i] = reconvert(engineRow.getFields()[i], rowType.getFieldType(i));
        }
        return new AresRow(fields);
    }

    /**
     * Convert HashTrieMap to LinkedHashMap
     *
     * @param hashTrieMap HashTrieMap data
     * @param mapType fields type map
     * @return java.util.LinkedHashMap
     * @see HashTrieMap
     */
    private Map<Object, Object> reconvertMap(HashTrieMap<?, ?> hashTrieMap, MapType<?, ?> mapType) {
        if (hashTrieMap == null || hashTrieMap.isEmpty()) {
            return Collections.emptyMap();
        }
        int num = hashTrieMap.size();
        Map<Object, Object> newMap = new LinkedHashMap<>(num);
        AresDataType<?> keyType = mapType.getKeyType();
        AresDataType<?> valueType = mapType.getValueType();
        scala.collection.immutable.List<?> keyList = hashTrieMap.keySet().toList();
        scala.collection.immutable.List<?> valueList = hashTrieMap.values().toList();
        for (int i = 0; i < num; i++) {
            Object key = keyList.apply(i);
            Object value = valueList.apply(i);
            key = reconvert(key, keyType);
            value = reconvert(value, valueType);
            newMap.put(key, value);
        }
        return newMap;
    }

    /**
     * Convert WrappedArray.ofRef to Objects array
     *
     * @param arrayData WrappedArray.ofRef data
     * @param arrayType fields type array
     * @return Objects array
     * @see WrappedArray.ofRef
     */
    private Object reconvertArray(WrappedArray.ofRef<?> arrayData, ArrayType<?, ?> arrayType) {
        if (arrayData == null || arrayData.isEmpty()) {
            return Collections.emptyList().toArray();
        }
        Object[] newArray = new Object[arrayData.size()];
        for (int i = 0; i < arrayData.size(); i++) {
            newArray[i] = reconvert(arrayData.apply(i), arrayType.getElementType());
        }
        return newArray;
    }
}
