package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.github.ares.spark.function.utils.TypeUtil.handleStringType;
import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;

@AutoService(SparkFuncInterface.class)
public class Crc32 implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "CRC32";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.LONG_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());

        Object arg = args.get(0);
        if (arg == null) {
            return null;
        }
        byte[] bytes;
        if (arg instanceof String) {
            bytes = ((String) arg).getBytes(StandardCharsets.UTF_8);
        } else if (arg instanceof byte[]) {
            bytes = (byte[]) arg;
        } else {
            throw new AresException("cannot resolve 'crc32(" + handleStringType(arg) + ")' due to data type mismatch: " +
                    "argument requires binary type, however, " + handleStringType(arg) + " is of " + arg.getClass().getSimpleName() + " type");
        }

        org.apache.spark.sql.catalyst.expressions.Crc32 crc32 = new org.apache.spark.sql.catalyst.expressions.Crc32(null);
        return crc32.nullSafeEval(bytes);
    }
}
