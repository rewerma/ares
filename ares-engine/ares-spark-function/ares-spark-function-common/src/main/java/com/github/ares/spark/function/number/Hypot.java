package com.github.ares.spark.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;


@AutoService(SparkFuncInterface.class)
public class Hypot implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "HYPOT";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.DOUBLE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        Number arg0 = toNumber(args.get(0));
        Number arg1 = toNumber(args.get(1));
        if (arg0 == null || arg1 == null) {
            return null;
        }
        org.apache.spark.sql.catalyst.expressions.Hypot hypot = new org.apache.spark.sql.catalyst.expressions.Hypot(null, null);
        return hypot.nullSafeEval(arg0.doubleValue(), arg1.doubleValue());
    }
}
