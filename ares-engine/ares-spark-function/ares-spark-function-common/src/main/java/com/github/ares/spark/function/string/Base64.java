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
public class Base64 implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "BASE64";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());

        Object arg = args.get(0);

        return rawToBase64(arg);
    }

    public static Object rawToBase64(Object arg) {
        if (arg == null) {
            return null;
        }
        org.apache.spark.sql.catalyst.expressions.Base64 base64 = new org.apache.spark.sql.catalyst.expressions.Base64(null);
        if (arg instanceof byte[]) {
            return base64.nullSafeEval(arg).toString();
        } else if (arg instanceof String) {
            return base64.nullSafeEval(((String) arg).getBytes(StandardCharsets.UTF_8)).toString();
        } else {
            throw new AresException("cannot resolve 'base64(" + handleStringType(arg) + ")' due to data type mismatch: " +
                    "argument requires binary type, however, '" + handleStringType(arg) + "' is of " + arg.getClass().getSimpleName() + " type");
        }
    }
}
