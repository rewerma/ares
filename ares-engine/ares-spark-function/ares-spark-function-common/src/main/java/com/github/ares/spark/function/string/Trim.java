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
public class Trim implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "TRIM";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{1, 2}, args.size());

        String arg = toStr(args.get(0));
        if (arg == null) {
            return null;
        }
        UTF8String srcString = UTF8String.fromString(arg);
        UTF8String trimString = null;
        if (args.size() == 2) {
            trimString = UTF8String.fromString(toStr(args.get(1)));
        }
        if (trimString == null) {
            return srcString.trim().toString();
        } else {
            return srcString.trim(trimString).toString();
        }
    }
}
