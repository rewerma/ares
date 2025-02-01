package com.github.ares.api.table.type;

import java.math.BigDecimal;
import java.util.Objects;

public final class DecimalType extends BasicType<BigDecimal> {
    private static final long serialVersionUID = -1L;

    private final int precision;

    private final int scale;

    public DecimalType(int precision, int scale) {
        super(BigDecimal.class, SqlType.DECIMAL);
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DecimalType)) {
            return false;
        }
        DecimalType that = (DecimalType) o;
        return this.precision == that.precision && this.scale == that.scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(precision, scale);
    }

    @Override
    public String toString() {
        return String.format("Decimal(%d, %d)", precision, scale);
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }
}
