package com.github.ares.spark.connector.serialization;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.MapType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connector.serialization.RowConverter;
import com.github.ares.spark.connector.utils.InstantConverterUtils;
import com.github.ares.spark.connector.utils.TypeConverterUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.MutableAny;
import org.apache.spark.sql.catalyst.expressions.MutableBoolean;
import org.apache.spark.sql.catalyst.expressions.MutableByte;
import org.apache.spark.sql.catalyst.expressions.MutableDouble;
import org.apache.spark.sql.catalyst.expressions.MutableFloat;
import org.apache.spark.sql.catalyst.expressions.MutableInt;
import org.apache.spark.sql.catalyst.expressions.MutableLong;
import org.apache.spark.sql.catalyst.expressions.MutableShort;
import org.apache.spark.sql.catalyst.expressions.MutableValue;
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Tuple2;
import scala.collection.immutable.HashMap.HashTrieMap;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public final class InternalRowConverter extends RowConverter<InternalRow> {

    public InternalRowConverter(AresDataType<?> dataType) {
        super(dataType);
    }

    @Override
    public InternalRow convert(AresRow aresRow) throws IOException {
        validate(aresRow);
        return (InternalRow) convert(aresRow, dataType);
    }

    private static Object convert(Object field, AresDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case ROW:
                AresRow aresRow = (AresRow) field;
                AresRowType rowType = (AresRowType) dataType;
                return convert(aresRow, rowType);
            case DATE:
                return (int) ((LocalDate) field).toEpochDay();
            case TIME:
                return ((LocalTime) field).toNanoOfDay();
            case TIMESTAMP:
                return InstantConverterUtils.toEpochMicro(
                        Timestamp.valueOf((LocalDateTime) field).toInstant());
            case MAP:
                return convertMap((Map<?, ?>) field, (MapType<?, ?>) dataType);
            case STRING:
                return UTF8String.fromString((String) field);
            case DECIMAL:
                return Decimal.apply((BigDecimal) field);
            case ARRAY:
                // if string array, we need to covert every item in array from String to UTF8String
                if (((ArrayType<?, ?>) dataType).getElementType().equals(BasicType.STRING_TYPE)) {
                    Object[] fields = (Object[]) field;
                    Object[] objects =
                            Arrays.stream(fields)
                                    .map(v -> UTF8String.fromString((String) v))
                                    .toArray();
                    return ArrayData.toArrayData(objects);
                }
                // except string, now only support convert boolean int tinyint smallint bigint float
                // double, because Ares Array only support these types
                return ArrayData.toArrayData(field);
            default:
                if (field instanceof scala.Some) {
                    return ((scala.Some<?>) field).get();
                }
                return field;
        }
    }

    private static InternalRow convert(AresRow aresRow, AresRowType rowType) {
        int arity = rowType.getTotalFields();
        MutableValue[] values = new MutableValue[arity];
        for (int i = 0; i < arity; i++) {
            values[i] = createMutableValue(rowType.getFieldType(i));
            if (TypeConverterUtils.ROW_KIND_FIELD.equals(rowType.getFieldName(i))) {
                values[i].update(aresRow.getRowKind().toByteValue());
            } else {
                Object fieldValue = convert(aresRow.getField(i), rowType.getFieldType(i));
                if (fieldValue != null) {
                    values[i].update(fieldValue);
                }
            }
        }
        return new SpecificInternalRow(values);
    }

    private static ArrayBasedMapData convertMap(Map<?, ?> mapData, MapType<?, ?> mapType) {
        if (mapData == null || mapData.isEmpty()) {
            return ArrayBasedMapData.apply(new Object[] {}, new Object[] {});
        }
        AresDataType<?> keyType = mapType.getKeyType();
        AresDataType<?> valueType = mapType.getValueType();
        Map<Object, Object> newMap = new HashMap<>(mapData.size());
        mapData.forEach(
                (key, value) -> newMap.put(convert(key, keyType), convert(value, valueType)));
        Object[] keys = newMap.keySet().toArray();
        Object[] values = newMap.values().toArray();
        return ArrayBasedMapData.apply(keys, values);
    }

    private static Map<Object, Object> reconvertMap(MapData mapData, MapType<?, ?> mapType) {
        if (mapData == null || mapData.numElements() == 0) {
            return Collections.emptyMap();
        }
        Map<Object, Object> newMap = new HashMap<>(mapData.numElements());
        int num = mapData.numElements();
        AresDataType<?> keyType = mapType.getKeyType();
        AresDataType<?> valueType = mapType.getValueType();
        Object[] keys = mapData.keyArray().toObjectArray(TypeConverterUtils.convert(keyType));
        Object[] values = mapData.valueArray().toObjectArray(TypeConverterUtils.convert(valueType));
        for (int i = 0; i < num; i++) {
            keys[i] = reconvert(keys[i], keyType);
            values[i] = reconvert(values[i], valueType);
            newMap.put(keys[i], values[i]);
        }
        return newMap;
    }

    private static Map<Object, Object> reconvertMap(
            HashTrieMap<?, ?> hashTrieMap, MapType<?, ?> mapType) {
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

    private static MutableValue createMutableValue(AresDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case BOOLEAN:
                return new MutableBoolean();
            case TINYINT:
                return new MutableByte();
            case SMALLINT:
                return new MutableShort();
            case INT:
            case DATE:
                return new MutableInt();
            case BIGINT:
            case TIME:
            case TIMESTAMP:
                return new MutableLong();
            case FLOAT:
                return new MutableFloat();
            case DOUBLE:
                return new MutableDouble();
            default:
                return new MutableAny();
        }
    }

    @Override
    public AresRow reconvert(InternalRow engineRow) throws IOException {
        return (AresRow) reconvert(engineRow, dataType);
    }

    private static Object reconvert(Object field, AresDataType<?> dataType) {
        if (field == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case ROW:
                return reconvert((InternalRow) field, (AresRowType) dataType);
            case DATE:
                if (field instanceof Date) {
                    return ((Date) field).toLocalDate();
                }
                return LocalDate.ofEpochDay((int) field);
            case TIME:
                if (field instanceof Timestamp) {
                    return LocalTime.ofNanoOfDay(((Timestamp) field).getNanos());
                }
                return LocalTime.ofNanoOfDay((long) field);
            case TIMESTAMP:
                if (field instanceof Timestamp) {
                    return ((Timestamp) field).toLocalDateTime();
                }
                return Timestamp.from(InstantConverterUtils.ofEpochMicro((long) field))
                        .toLocalDateTime();
            case MAP:
                if (field instanceof MapData) {
                    return reconvertMap((MapData) field, (MapType<?, ?>) dataType);
                } else if (field instanceof HashTrieMap) {
                    return reconvertMap((HashTrieMap<?, ?>) field, (MapType<?, ?>) dataType);
                } else {
                    throw new AresException(
                            String.format(
                                    "Ares unsupported Spark internal Map type: %s ",
                                    field.getClass()));
                }
            case STRING:
                return field.toString();
            case DECIMAL:
                if (field instanceof Decimal) {
                    return ((Decimal) field).toJavaBigDecimal();
                } else if (field instanceof BigDecimal) {
                    return field;
                }
            case ARRAY:
                if (field instanceof ArrayData) {
                    return reconvertArray((ArrayData) field, (ArrayType<?, ?>) dataType);
                } else if (field instanceof WrappedArray.ofRef) {
                    return reconvertArray(
                            (WrappedArray.ofRef<?>) field, (ArrayType<?, ?>) dataType);
                } else {
                    throw new AresException(
                            String.format(
                                    "Ares unsupported Spark internal Array type: %s ",
                                    field.getClass()));
                }
            default:
                return field;
        }
    }

    private static AresRow reconvert(InternalRow engineRow, AresRowType rowType) {
        Object[] fields = new Object[engineRow.numFields()];
        for (int i = 0; i < engineRow.numFields(); i++) {
            fields[i] =
                    reconvert(
                            engineRow.get(i, TypeConverterUtils.convert(rowType.getFieldType(i))),
                            rowType.getFieldType(i));
        }
        return new AresRow(fields);
    }

    private static Object reconvertArray(ArrayData arrayData, ArrayType<?, ?> arrayType) {
        if (arrayData == null || arrayData.numElements() == 0) {
            return Collections.emptyList().toArray();
        }
        Object[] newArray = new Object[arrayData.numElements()];
        Object[] values =
                arrayData.toObjectArray(TypeConverterUtils.convert(arrayType.getElementType()));
        for (int i = 0; i < arrayData.numElements(); i++) {
            newArray[i] = reconvert(values[i], arrayType.getElementType());
        }
        return newArray;
    }

    private static Object reconvertArray(
            WrappedArray.ofRef<?> arrayData, ArrayType<?, ?> arrayType) {
        if (arrayData == null || arrayData.isEmpty()) {
            return Collections.emptyList().toArray();
        }
        Object[] newArray = new Object[arrayData.size()];
        for (int i = 0; i < arrayData.size(); i++) {
            newArray[i] = reconvert(arrayData.apply(i), arrayType.getElementType());
        }
        return newArray;
    }

    public Object[] convertToFields(InternalRow internalRow, StructType structType) {
        Object[] fields =
                Arrays.stream(((SpecificInternalRow) internalRow).values())
                        .map(MutableValue::boxed)
                        .toArray();
        int len = structType.fields().length;
        for (int i = 0; i < len; i++) {
            DataType dataType = structType.fields()[i].dataType();
            fields[i] = convertToField(fields[i], dataType);
        }
        return fields;
    }

    private Object convertToField(Object internalRowField, DataType dataType) {
        if (dataType == DataTypes.TimestampType && internalRowField instanceof Long) {
            return Timestamp.from(InstantConverterUtils.ofEpochMicro((long) internalRowField));
        } else if (dataType == DataTypes.DateType && internalRowField instanceof Integer) {
            return Date.valueOf(LocalDate.ofEpochDay((int) internalRowField));
        } else if (dataType == DataTypes.StringType && internalRowField instanceof UTF8String) {
            return internalRowField.toString();
        } else if (dataType instanceof org.apache.spark.sql.types.MapType
                && internalRowField instanceof MapData) {
            MapData mapData = (MapData) internalRowField;

            scala.collection.immutable.HashMap<Object, Object> newMap =
                    new scala.collection.immutable.HashMap<>();

            if (mapData.numElements() == 0) {
                return newMap;
            }
            org.apache.spark.sql.types.MapType mapType =
                    (org.apache.spark.sql.types.MapType) dataType;

            int num = mapData.numElements();
            Object[] keys = mapData.keyArray().toObjectArray(mapType.keyType());
            Object[] values = mapData.valueArray().toObjectArray(mapType.valueType());
            for (int i = 0; i < num; i++) {
                keys[i] = convertToField(keys[i], mapType.keyType());
                values[i] = convertToField(values[i], mapType.valueType());
                Tuple2<Object, Object> tuple2 = new Tuple2<>(keys[i], values[i]);
                newMap = newMap.$plus(tuple2);
            }
            return newMap;
        } else if (dataType instanceof org.apache.spark.sql.types.ArrayType
                && internalRowField instanceof ArrayData) {
            ArrayData arrayData = (ArrayData) internalRowField;
            if (arrayData.numElements() == 0) {
                return new WrappedArray.ofRef<>(new Object[0]);
            }
            org.apache.spark.sql.types.ArrayType arrayType =
                    (org.apache.spark.sql.types.ArrayType) dataType;
            Object[] values = arrayData.array();
            int num = arrayData.numElements();
            for (int i = 0; i < num; i++) {
                values[i] = convertToField(values[i], arrayType.elementType());
            }
            return new WrappedArray.ofRef<>(values);
        }
        return internalRowField;
    }
}
