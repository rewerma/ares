package com.github.ares.spark.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.math.BigDecimal;
import java.util.List;

@AutoService(SparkFuncInterface.class)
public class Greatest implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "GREATEST";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.INT_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        if (args.isEmpty()) {
            throw new AresException(
                    String.format(
                            "The `greatest` requires > 0 parameters but the actual number is %d", args.size()));
        }
        boolean isAllInt = isAllInt(args);
        if (isAllInt) {
            return args.stream().map(v -> ((Number) v).longValue()).max(Long::compare).get();
        }
        if (isDouble(args)) {
            return args.stream().map(v -> ((Number) v).doubleValue()).max(Double::compare).get();
        }
        return null;
    }

    private static boolean isAllInt(List<Object> args) {
        for (Object arg : args) {
            if (!(arg instanceof Integer) && !(arg instanceof Short) && !(arg instanceof Long) && !(arg instanceof Byte)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isDouble(List<Object> args) {
        for (Object arg : args) {
            if (!(arg instanceof Number)) {
                throw new AresException("The `greatest` function only supports numeric types.");
            }
            if ((arg instanceof Double) || (arg instanceof Float) || (arg instanceof BigDecimal)) {
                return true;
            }
        }
        return false;
    }
}
