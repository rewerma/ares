package com.github.ares.spark.function.utils;

import com.github.ares.common.exceptions.AresException;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.Decimal;
import scala.Tuple2;

import java.math.BigDecimal;

public class TypeUtil {
    public static Tuple2<Expression, Object> getNumberType(Object value) {
        return getNumberType(value, Double.class);
    }

    public static Tuple2<Expression, Object> getNumberType(Object value, Class<? extends Number> defaultType) {
        Expression typeExpr;
        if (value instanceof Integer) {
            typeExpr = new IntegerTypeExpression();
        } else if (value instanceof Long) {
            typeExpr = new LongTypeExpression();
        } else if (value instanceof Short) {
            typeExpr = new ShortTypeExpression();
        } else if (value instanceof Byte) {
            typeExpr = new ByteTypeExpression();
        } else if (value instanceof Float) {
            typeExpr = new FloatTypeExpression();
        } else if (value instanceof Double) {
            typeExpr = new DoubleTypeExpression();
        } else if (value instanceof BigDecimal) {
            int scale = ((BigDecimal) value).scale();
            int precision = ((BigDecimal) value).precision();
            typeExpr = new DecimalTypeExpression(precision, scale);
            Decimal decimal = new Decimal();
            value = decimal.set(new scala.math.BigDecimal((BigDecimal) value));
        } else {
            if (defaultType == null) {
                throw new AresException("cannot resolve '" + value + "' due to data type mismatch: argument requires " +
                        "number type, however, " + value + " is of " + value.getClass().getSimpleName() + " type.");
            }
            if (defaultType == Integer.class) {
                value = ((Number) value).intValue();
                typeExpr = new IntegerTypeExpression();
            } else if (defaultType == Long.class) {
                value = ((Number) value).longValue();
                typeExpr = new LongTypeExpression();
            } else if (defaultType == Short.class) {
                value = ((Number) value).shortValue();
                typeExpr = new ShortTypeExpression();
            } else if (defaultType == Byte.class) {
                value = ((Number) value).byteValue();
                typeExpr = new ByteTypeExpression();
            } else if (defaultType == Float.class) {
                value = ((Number) value).floatValue();
                typeExpr = new FloatTypeExpression();
            } else if (defaultType == Double.class) {
                value = ((Number) value).doubleValue();
                typeExpr = new DoubleTypeExpression();
            } else {
                throw new AresException("cannot convert '" + value + "' to " + defaultType.getSimpleName() + "  type");
            }
        }
        return new Tuple2<>(typeExpr, value);
    }

    public static Object handleStringType(Object value) {
        if (value instanceof String) {
            return "'" + value + "'";
        }
        return value;
    }
}
