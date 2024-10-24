package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.Sha1;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;

@AutoService(SparkFuncInterface.class)
public class Sha implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "SHA";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());

        Object arg = args.get(0);
        if (arg == null) {
            return null;
        }
        byte[] bytes;
        if (arg instanceof byte[]) {
            bytes = (byte[]) arg;
        } else if (arg instanceof String) {
            bytes = arg.toString().getBytes(StandardCharsets.UTF_8);
        } else {
            throw new AresException("Cannot resolve \"" + functionName() + "(" + arg + ")\" due to data type mismatch: " +
                    "Parameter 1 requires the \"BINARY\" type, however \"" + arg + "\" has the type \"" + arg.getClass().getSimpleName() + "\".");
        }
        Sha1 sha1 = new Sha1(null);
        return sha1.nullSafeEval(bytes).toString();
    }
}
