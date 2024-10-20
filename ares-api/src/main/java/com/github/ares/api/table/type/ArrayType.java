package com.github.ares.api.table.type;

import java.util.Objects;

public class ArrayType <T, E> implements AresDataType<T> {
    private static final long serialVersionUID = 1L;

    public static final ArrayType<String[], String> STRING_ARRAY_TYPE =
            new ArrayType<>(String[].class, BasicType.STRING_TYPE);
    public static final ArrayType<Boolean[], Boolean> BOOLEAN_ARRAY_TYPE =
            new ArrayType<>(Boolean[].class, BasicType.BOOLEAN_TYPE);
    public static final ArrayType<Byte[], Byte> BYTE_ARRAY_TYPE =
            new ArrayType<>(Byte[].class, BasicType.BYTE_TYPE);
    public static final ArrayType<Short[], Short> SHORT_ARRAY_TYPE =
            new ArrayType<>(Short[].class, BasicType.SHORT_TYPE);
    public static final ArrayType<Integer[], Integer> INT_ARRAY_TYPE =
            new ArrayType<>(Integer[].class, BasicType.INT_TYPE);
    public static final ArrayType<Long[], Long> LONG_ARRAY_TYPE =
            new ArrayType<>(Long[].class, BasicType.LONG_TYPE);
    public static final ArrayType<Float[], Float> FLOAT_ARRAY_TYPE =
            new ArrayType<>(Float[].class, BasicType.FLOAT_TYPE);
    public static final ArrayType<Double[], Double> DOUBLE_ARRAY_TYPE =
            new ArrayType<>(Double[].class, BasicType.DOUBLE_TYPE);

    // --------------------------------------------------------------------------------------------

    private final Class<T> arrayClass;
    private final BasicType<E> elementType;

    private ArrayType(Class<T> arrayClass, BasicType<E> elementType) {
        this.arrayClass = arrayClass;
        this.elementType = elementType;
    }

    public BasicType<E> getElementType() {
        return elementType;
    }

    @Override
    public Class<T> getTypeClass() {
        return arrayClass;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.ARRAY;
    }

    @Override
    public int hashCode() {
        return Objects.hash(arrayClass, elementType);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ArrayType)) {
            return false;
        }
        ArrayType<?, ?> that = (ArrayType<?, ?>) obj;
        return Objects.equals(arrayClass, that.arrayClass)
                && Objects.equals(elementType, that.elementType);
    }

    @Override
    public String toString() {
        return String.format("ARRAY<%s>", elementType);
    }
}
