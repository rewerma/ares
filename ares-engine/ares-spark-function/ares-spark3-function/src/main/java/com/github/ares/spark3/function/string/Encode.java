package com.github.ares.spark3.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;


@AutoService(SparkFuncInterface.class)
public class Encode implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "ENCODE";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return ArrayType.BYTE_ARRAY_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());

        String arg = toStr(args.get(0));
        String arg1 = toStr(args.get(1));
        if (arg == null && arg1 == null) {
            return null;
        }
        org.apache.spark.sql.catalyst.expressions.Encode encode = new org.apache.spark.sql.catalyst.expressions.Encode(null, null);
        return encode.nullSafeEval(UTF8String.fromString(arg), UTF8String.fromString(arg1));
    }
}
