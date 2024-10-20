package com.github.ares.spark3.function.system;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.spark.function.string.Base64;
import com.github.ares.spark.function.string.Unbase64;
import com.github.ares.spark.function.string.Unhex;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class ToBinary implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "TO_BINARY";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return ArrayType.BYTE_ARRAY_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{1, 2}, args.size());
        String arg1 = toStr(args.get(0));
        if (arg1 == null) {
            return null;
        }
        String format = "UTF-8";
        if (args.size() == 2) {
            format = toStr(args.get(1));
        }
        if (format == null) {
            return null;
        }
        switch (format.toUpperCase()) {
            case "UTF-8":
            case "UTF8":
                return arg1.getBytes(StandardCharsets.UTF_8);
            case "BASE64":
                return Unbase64.unbase64(arg1);
            case "HEX":
                return Unhex.unhex(arg1);
            default:
                throw new AresException("Cannot resolve \"to_binary(" + arg1 + ", " + format + ")\" due to data type mismatch: " +
                        "The fmt value must to be a case-insensitive \"STRING\" literal of 'hex', 'utf-8', 'utf8', or 'base64', but got '" + format + "'.");
        }
    }
}
