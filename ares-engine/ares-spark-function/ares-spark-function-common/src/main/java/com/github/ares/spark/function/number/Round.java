package com.github.ares.spark.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;

@AutoService(SparkFuncInterface.class)
public class Round implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "ROUND";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return argTypes.get(0);
    }

    @Override
    public Object evaluate(List<Object> args) {
        return evaluate(args, RoundingMode.HALF_UP);
    }

    public Object evaluate(List<Object> args, RoundingMode roundingMode) {
        validateArgCount(functionName(), new int[]{1, 2}, args.size());
        Number arg0 = toNumber(args.get(0));
        if (arg0 == null) {
            return null;
        }
        int scale = 0;
        if (args.size() == 2) {
            Number arg1 = toNumber(args.get(1));
            scale = arg1.intValue();
        }

        return round(arg0, scale, roundingMode);
    }

    public static Object round(Number arg, int scale, RoundingMode roundingMode) {
        if (arg instanceof Byte) {
            return arg;
        } else if (arg instanceof Short) {
            return arg;
        } else if (arg instanceof Integer) {
            return arg;
        } else if (arg instanceof Long) {
            return arg;
        } else if (arg instanceof Float) {
            if (scale > 0) {
                return BigDecimal.valueOf(arg.doubleValue()).setScale(scale, roundingMode).floatValue();
            } else {
                return BigDecimal.valueOf(arg.doubleValue()).setScale(scale, roundingMode).longValue();
            }
        } else if (arg instanceof Double) {
            if (scale > 0) {
                return BigDecimal.valueOf(arg.doubleValue()).setScale(scale, roundingMode).doubleValue();
            } else {
                return BigDecimal.valueOf(arg.doubleValue()).setScale(scale, roundingMode).longValue();
            }
        } else if (arg instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) arg;
            return new BigDecimal(decimal.toString()).setScale(decimal.scale(), roundingMode);
        }
        return null;
    }
}
