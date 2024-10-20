package com.github.ares.spark.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;


@AutoService(SparkFuncInterface.class)
public class Pow implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "POW";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.DOUBLE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        Number arg1 = toNumber(args.get(0));
        Number arg2 = toNumber(args.get(1));
        if (arg1 == null || arg2 == null) {
            return null;
        }
        return Math.pow(arg1.doubleValue(), arg2.doubleValue());
    }
}
