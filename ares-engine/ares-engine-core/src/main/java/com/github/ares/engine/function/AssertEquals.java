package com.github.ares.engine.function;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.UdfInterface;
import com.google.auto.service.AutoService;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@AutoService(UdfInterface.class)
public class AssertEquals implements UdfInterface {
    @Override
    public String functionName() {
        return "ASSERT_EQUALS";
    }

    @Override
    public AresDataType<?> resultType() {
        return BasicType.STRING_TYPE;
    }

    @Override
    public List<AresDataType<?>> argTypes() {
        return Arrays.asList(BasicType.ANY_TYPE, BasicType.ANY_TYPE);
    }

    @Override
    public Object evaluate(List<Object> args) {
        if (args.size() != 2) {
            throw new AresException("ASSERT function expects two argument");
        }
        Object arg1 = args.get(0);
        Object arg2 = args.get(1);
        if (arg1 == null && arg2 == null) {
            return arg1;
        }
        if (sameValue(arg1, arg2)) {
            return arg1;
        } else {
            if (arg1 instanceof String) {
                arg1 = "'" + arg1 + "'";
            }
            if (arg2 instanceof String) {
                arg2 = "'" + arg2 + "'";
            }
            throw new AresException("ASSERT_EQUALS failed: " + arg1 + " != " + arg2);
        }
    }

    /**
     * Spark functions often return {@code Long} for values that still fit in an {@code int}
     * literal. Compare numbers by numeric value, and byte arrays by content.
     */
    static boolean sameValue(Object left, Object right) {
        if (Objects.equals(left, right)) {
            return true;
        }
        if (left instanceof byte[] && right instanceof byte[]) {
            return Arrays.equals((byte[]) left, (byte[]) right);
        }
        if (left instanceof Number && right instanceof Number) {
            return toBigDecimal((Number) left).compareTo(toBigDecimal((Number) right)) == 0;
        }
        return false;
    }

    private static BigDecimal toBigDecimal(Number number) {
        if (number instanceof BigDecimal) {
            return (BigDecimal) number;
        }
        if (number instanceof BigInteger) {
            return new BigDecimal((BigInteger) number);
        }
        if (number instanceof Double || number instanceof Float) {
            return BigDecimal.valueOf(number.doubleValue());
        }
        return BigDecimal.valueOf(number.longValue());
    }
}
