package com.github.ares.api.table.type;

import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.AresException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class AresDataTypeHelper {
    public static AresDataType<?> getAresDataType(PlType type) {
        switch (type.getType()) {
            case LONG:
                return BasicType.LONG_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case INT:
                return BasicType.INT_TYPE;
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case BYTE:
                return BasicType.BYTE_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case NUMERIC:
                int precision = type.getPrecision() == null ? 10 : type.getPrecision();
                int scale = type.getScale() == null ? 0 : type.getScale();
                return new DecimalType(precision, scale);
            case VARCHAR:
                return BasicType.STRING_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case BYTES:
                return ArrayType.BYTE_ARRAY_TYPE;
            default:
                throw new AresException("Unsupported data type: " + type);
        }
    }

    public static AresDataType<?> getAresDataType(Object value) {
        Class<?> clazz = value.getClass();
        if (clazz == Boolean.class) {
            return BasicType.BOOLEAN_TYPE;
        } else if (clazz == Byte.class) {
            return BasicType.BYTE_TYPE;
        } else if (clazz == Short.class) {
            return BasicType.SHORT_TYPE;
        } else if (clazz == Integer.class) {
            return BasicType.INT_TYPE;
        } else if (clazz == Long.class) {
            return BasicType.LONG_TYPE;
        } else if (clazz == Float.class) {
            return BasicType.FLOAT_TYPE;
        } else if (clazz == Double.class) {
            return BasicType.DOUBLE_TYPE;
        } else if (clazz == BigDecimal.class) {
            BigDecimal bigDecimal = (BigDecimal) value;
            return new DecimalType(bigDecimal.precision(), bigDecimal.scale());
        } else if (clazz == String.class) {
            return BasicType.STRING_TYPE;
        } else if (clazz == LocalDate.class) {
            return LocalTimeType.LOCAL_DATE_TYPE;
        } else if (clazz == LocalDateTime.class) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        } else if (clazz == byte[].class) {
            return ArrayType.BYTE_ARRAY_TYPE;
        } else {
            throw new AresException("Unsupported data type: " + clazz.getName());
        }
    }
}
