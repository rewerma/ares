package com.github.ares.spark.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;

@AutoService(SparkFuncInterface.class)
public class Bin implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "BIN";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());
        Number arg = toNumber(args.get(0));
        if (arg == null) {
            return null;
        }
        org.apache.spark.sql.catalyst.expressions.Bin bin = new org.apache.spark.sql.catalyst.expressions.Bin(null);
        return bin.nullSafeEval(arg.longValue()).toString();
    }
}
