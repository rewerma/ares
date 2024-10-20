package com.github.ares.spark.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;


@AutoService(SparkFuncInterface.class)
public class Sqrt implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "SQRT";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.DOUBLE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());
        Number arg1 = toNumber(args.get(0));
        if (arg1 == null) {
            return null;
        }
        org.apache.spark.sql.catalyst.expressions.Sqrt sqrt = new org.apache.spark.sql.catalyst.expressions.Sqrt(null);
        return sqrt.nullSafeEval(arg1.doubleValue());
    }
}
