package com.github.ares.api.table.type;

import java.util.Objects;

public class BasicType<T> implements AresDataType<T> {
    private static final long serialVersionUID = 1L;

    public static final BasicType<String> STRING_TYPE =
            new BasicType<>(String.class, SqlType.STRING);
    public static final BasicType<Boolean> BOOLEAN_TYPE =
            new BasicType<>(Boolean.class, SqlType.BOOLEAN);
    public static final BasicType<Byte> BYTE_TYPE = new BasicType<>(Byte.class, SqlType.TINYINT);
    public static final BasicType<Short> SHORT_TYPE =
            new BasicType<>(Short.class, SqlType.SMALLINT);
    public static final BasicType<Integer> INT_TYPE = new BasicType<>(Integer.class, SqlType.INT);
    public static final BasicType<Long> LONG_TYPE = new BasicType<>(Long.class, SqlType.BIGINT);
    public static final BasicType<Float> FLOAT_TYPE = new BasicType<>(Float.class, SqlType.FLOAT);
    public static final BasicType<Double> DOUBLE_TYPE =
            new BasicType<>(Double.class, SqlType.DOUBLE);
    public static final BasicType<Void> VOID_TYPE = new BasicType<>(Void.class, SqlType.NULL);
    public static final BasicType<AnyType> ANY_TYPE = new BasicType<>(AnyType.class, SqlType.NULL);

    // --------------------------------------------------------------------------------------------

    /** The physical type class. */
    private final Class<T> typeClass;

    private final SqlType sqlType;

    protected BasicType(Class<T> typeClass, SqlType sqlType) {
        this.typeClass = typeClass;
        this.sqlType = sqlType;
    }

    @Override
    public Class<T> getTypeClass() {
        return this.typeClass;
    }

    @Override
    public SqlType getSqlType() {
        return this.sqlType;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof BasicType)) {
            return false;
        }
        BasicType<?> that = (BasicType<?>) obj;
        return Objects.equals(typeClass, that.typeClass) && Objects.equals(sqlType, that.sqlType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeClass, sqlType);
    }

    @Override
    public String toString() {
        return sqlType.toString();
    }
}
