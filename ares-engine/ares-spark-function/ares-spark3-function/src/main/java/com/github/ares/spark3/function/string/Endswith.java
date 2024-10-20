package com.github.ares.spark3.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.EndsWith;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class Endswith implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "ENDSWITH";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.BOOLEAN_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        Object arg1 = args.get(0);
        Object arg2 = args.get(1);
        if (arg1 == null || arg2 == null) {
            return null;
        }
        UTF8String v1;
        UTF8String v2;
        if (arg1 instanceof byte[]) {
            v1 = UTF8String.fromBytes((byte[]) arg1);
        } else {
            v1 = UTF8String.fromString(arg1.toString());
        }
        if (arg2 instanceof byte[]) {
            v2 = UTF8String.fromBytes((byte[]) arg2);
        } else {
            v2 = UTF8String.fromString(arg2.toString());
        }
        EndsWith endsWith = new EndsWith(null, null);
        return endsWith.nullSafeEval(v1, v2);
    }
}
