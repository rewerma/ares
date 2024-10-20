package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class Ascii implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "ASCII";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.INT_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());

        String arg = toStr(args.get(0));
        if (arg == null) {
            return null;
        }
        UTF8String utf8String = UTF8String.fromString(arg);
        org.apache.spark.sql.catalyst.expressions.Ascii ascii = new org.apache.spark.sql.catalyst.expressions.Ascii(null);
        return ascii.nullSafeEval(utf8String);
    }
}
