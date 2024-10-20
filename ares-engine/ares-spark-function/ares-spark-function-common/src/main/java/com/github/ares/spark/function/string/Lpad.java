package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.StringLPad;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class Lpad implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "LPAD";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{2, 3}, args.size());
        String arg1 = toStr(args.get(0));
        Number arg2 = toNumber(args.get(1));
        if (arg1 == null || arg2 == null) {
            return null;
        }
        String pad = " ";
        if (args.size() == 3) {
            pad = toStr(args.get(2));
        }
        StringLPad stringLPad = new StringLPad(null, null, null);
        return stringLPad.nullSafeEval(UTF8String.fromString(arg1), arg2.intValue(), UTF8String.fromString(pad));
    }
}
