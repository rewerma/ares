package com.github.ares.spark3.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class Decode implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "DECODE";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return ArrayType.BYTE_ARRAY_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());

        Object arg = args.get(0);
        String arg1 = toStr(args.get(1));
        if (arg == null && arg1 == null) {
            return null;
        }
        byte[] input = null;
        if (arg instanceof byte[]) {
            input = (byte[]) arg;
        } else if (arg instanceof String) {
            input = ((String) arg).getBytes(StandardCharsets.UTF_8);
        } else {
            throw new AresException("Cannot resolve \"decode(" + arg + ", " + arg1 + ")\" due to data type mismatch: " +
                    "Parameter 1 requires the \"BINARY\" type, " +
                    "however \"" + arg + "\" has the type \"" + arg.getClass().getSimpleName() + "\".");
        }

        try {
            return new String(input, arg1);
        } catch (UnsupportedEncodingException e) {
            throw new AresException(e);
        }
    }
}
