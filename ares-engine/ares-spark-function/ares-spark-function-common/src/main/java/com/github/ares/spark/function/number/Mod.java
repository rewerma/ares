package com.github.ares.spark.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresDataTypeHelper;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;


@AutoService(SparkFuncInterface.class)
public class Mod implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "MOD";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        validateArgCount(functionName(), 2, argTypes.size());
        return getDataType(argTypes.get(0), argTypes.get(1));
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        Number arg1 = toNumber(args.get(0));
        Number arg2 = toNumber(args.get(1));
        if (arg1 == null || arg2 == null) {
            return null;
        }
        if (arg2.intValue() == 0) {
            return null;
        }
        if (arg1 instanceof BigDecimal) {
            arg1 = ((BigDecimal) arg1).setScale(0, RoundingMode.HALF_UP).intValue();
        }
        if (arg2 instanceof BigDecimal) {
            arg2 = ((BigDecimal) arg2).setScale(0, RoundingMode.HALF_UP).intValue();
        }
        AresDataType<?> dataType = getDataType(arg1, arg2);
        if (dataType == BasicType.BYTE_TYPE) {
            return arg1.byteValue() % arg2.byteValue();
        } else if (dataType == BasicType.SHORT_TYPE) {
            return arg1.shortValue() % arg2.shortValue();
        } else if (dataType == BasicType.INT_TYPE) {
            return arg1.intValue() % arg2.intValue();
        } else if (dataType == BasicType.LONG_TYPE) {
            return arg1.longValue() % arg2.longValue();
        } else if (dataType == BasicType.FLOAT_TYPE) {
            BigDecimal decimal0 = BigDecimal.valueOf(arg1.floatValue());
            BigDecimal decimal1 = BigDecimal.valueOf(arg2.floatValue());
            return decimal0.remainder(decimal1).floatValue();
        } else if (dataType == BasicType.DOUBLE_TYPE) {
            BigDecimal decimal0 = BigDecimal.valueOf(arg1.doubleValue());
            BigDecimal decimal1 = BigDecimal.valueOf(arg2.doubleValue());
            return decimal0.remainder(decimal1).doubleValue();
        }
        return null;
    }

    public static AresDataType<?> getDataType(Number arg1, Number arg2) {
        AresDataType<?> type1 = AresDataTypeHelper.getAresDataType(arg1);
        AresDataType<?> type2 = AresDataTypeHelper.getAresDataType(arg2);
        return getDataType(type1, type2);
    }

    public static AresDataType<?> getDataType(AresDataType<?> type1, AresDataType<?> type2) {
        if (type1 instanceof DecimalType) {
            type1 = BasicType.INT_TYPE;
        }
        if (type2 instanceof DecimalType) {
            type2 = BasicType.INT_TYPE;
        }
        List<AresDataType<?>> numberTypes = Arrays.asList(BasicType.BYTE_TYPE, BasicType.SHORT_TYPE, BasicType.INT_TYPE, BasicType.LONG_TYPE,
                BasicType.FLOAT_TYPE, BasicType.DOUBLE_TYPE);
        int idx1 = numberTypes.indexOf(type1);
        int idx2 = numberTypes.indexOf(type2);
        int idxMax = Math.max(idx1, idx2);
        return numberTypes.get(idxMax);
    }
}
