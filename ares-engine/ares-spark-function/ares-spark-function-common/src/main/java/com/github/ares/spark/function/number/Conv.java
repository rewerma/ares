package com.github.ares.spark.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;

@AutoService(SparkFuncInterface.class)
public class Conv implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "CONV";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 3, args.size());
        if (args.get(0) == null || args.get(1) == null || args.get(2) == null) {
            return null;
        }
        Object arg = toNumber(args.get(0)).longValue();
        int arg1 = toNumber(args.get(1)).intValue();
        int arg2 = toNumber(args.get(2)).intValue();
        org.apache.spark.sql.catalyst.expressions.Conv conv = new org.apache.spark.sql.catalyst.expressions.Conv(null, null, null);
        return conv.nullSafeEval(UTF8String.fromString(arg.toString()), arg1, arg2).toString();
    }
}
