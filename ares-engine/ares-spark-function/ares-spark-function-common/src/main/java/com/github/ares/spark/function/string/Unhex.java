package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;


@AutoService(SparkFuncInterface.class)
public class Unhex implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "UNHEX";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return ArrayType.BYTE_ARRAY_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());

        String arg = toStr(args.get(0));
        if (arg == null) {
            return null;
        }
        return unhex(arg);
    }

    public static byte[] unhex(String hex) {
        org.apache.spark.sql.catalyst.expressions.Unhex unhex = new org.apache.spark.sql.catalyst.expressions.Unhex(null);
        return (byte[]) unhex.nullSafeEval(UTF8String.fromString(hex));
    }
}