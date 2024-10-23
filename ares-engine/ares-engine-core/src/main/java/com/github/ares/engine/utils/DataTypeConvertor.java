package com.github.ares.engine.utils;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.utils.DateTimeUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static com.github.ares.common.utils.DateTimeUtils.DATE_FORMATTER;
import static com.github.ares.engine.core.ExpressionExecutor.rawToHex;
import static com.github.ares.engine.utils.EngineUtil.handleQuoteIdentifier;

public class DataTypeConvertor {
    public static Serializable convert(String fieldName, PlType fieldType, Serializable originValue) {
        return convert(fieldName, fieldType, originValue, false);
    }

    public static Serializable convertWithIdentifier(String fieldName, PlType fieldType, Serializable originValue) {
        return convert(fieldName, fieldType, originValue, true);
    }

    public static Serializable convert(String fieldName, PlType fieldType, Serializable originValue, boolean isIdentifier) {
        if (originValue == null) {
            return null;
        }
        switch (fieldType.getType()) {
            case VARCHAR:
                if (isIdentifier) {
                    return handleQuoteIdentifier(originValue.toString());
                } else {
                    return objectToString(originValue);
                }
            case DATE:
                String dateStr;
                if (originValue instanceof LocalDate) {
                    LocalDate localDate = (LocalDate) originValue;
                    dateStr = localDate.format(DATE_FORMATTER);
                    dateStr = handleQuoteIdentifier(dateStr);
                } else if (originValue instanceof LocalDateTime) {
                    LocalDateTime localDateTime = (LocalDateTime) originValue;
                    dateStr = DateTimeUtils.localDateTimeToString(localDateTime);
                    dateStr = handleQuoteIdentifier(dateStr);
                } else {
                    if (isIdentifier) {
                        dateStr = handleQuoteIdentifier(originValue);
                    } else {
                        dateStr = objectToString(originValue);
                    }
                }

                return dateStr;
            case TIMESTAMP:
                String dateTimeStr;
                if (originValue instanceof LocalDate) {
                    LocalDateTime localDateTime = ((LocalDate) originValue).atStartOfDay();
                    dateTimeStr = DateTimeUtils.localDateTimeToString(localDateTime);
                    dateTimeStr = handleQuoteIdentifier(dateTimeStr);
                } else if (originValue instanceof LocalDateTime) {
                    LocalDateTime localDateTime = (LocalDateTime) originValue;
                    dateTimeStr = DateTimeUtils.localDateTimeToString(localDateTime);
                    dateTimeStr = handleQuoteIdentifier(dateTimeStr);
                } else {
                    if (isIdentifier) {
                        dateTimeStr = handleQuoteIdentifier(originValue);
                    } else {
                        dateTimeStr = objectToString(originValue);
                    }
                }
                return dateTimeStr;
            case BYTES:
                String resVal;
                if (originValue instanceof byte[]) {
                    resVal = rawToHex(originValue);
                    resVal = handleQuoteIdentifier(resVal);
                } else {
                    if (isIdentifier) {
                        resVal = handleQuoteIdentifier(originValue);
                    } else {
                        resVal = objectToString(originValue);
                    }
                }
                return resVal;
            case LONG:
                return Long.parseLong(objectToString(originValue).split("\\.")[0]);
            case BOOLEAN:
                if (originValue instanceof Boolean) {
                    return originValue;
                } else if (originValue instanceof Number) {
                    return ((Number) originValue).intValue() != 0;
                }
            case INT:
                return Integer.parseInt(objectToString(originValue).split("\\.")[0]);
            case SMALLINT:
                return Short.parseShort(objectToString(originValue).split("\\.")[0]);
            case BYTE:
                return Byte.parseByte(objectToString(originValue).split("\\.")[0]);
            case DOUBLE:
                return Double.parseDouble(objectToString(originValue));
            case FLOAT:
                return Float.parseFloat(objectToString(originValue));
            case NUMERIC:
                int scale = fieldType.getScale() != null ? fieldType.getScale() : 0;
                BigDecimal bigDecimal = new BigDecimal(objectToString(originValue));
                return bigDecimal.setScale(scale, RoundingMode.HALF_UP);
        }
        if (fieldName != null) {
            throw new AresException(String.format("Invalid param %s type: %s", fieldName, fieldType.getType().name()));
        } else {
            throw new AresException(String.format("Invalid type: %s", fieldType.getType().name()));
        }
    }

    private static String objectToString(Object obj) {
        if (obj instanceof Double) {
            BigDecimal bigDecimal = BigDecimal.valueOf((Double) obj);
            return bigDecimal.toPlainString();
        } else if (obj instanceof Float) {
            BigDecimal bigDecimal = BigDecimal.valueOf((Float) obj);
            return bigDecimal.toPlainString();
        } else {
            return obj.toString();
        }
    }

}
