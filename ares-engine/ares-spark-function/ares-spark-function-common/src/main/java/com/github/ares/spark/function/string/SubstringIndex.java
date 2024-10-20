package com.github.ares.spark.function.string;


import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.spark.function.utils.BinaryTypeExpression;
import com.github.ares.spark.function.utils.StringTypeExpression;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class SubstringIndex implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "SUBSTRING_INDEX";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{2, 3}, args.size());
        Object arg1 = args.get(0);
        String arg2 = toStr(args.get(1));
        Number arg3 = toNumber(args.get(2));
        if (arg1 == null || arg2 == null || arg3 == null) {
            return null;
        }

        String str = null;
        if (arg1 instanceof byte[]) {
            str = new String((byte[]) arg1, StandardCharsets.UTF_8);
        } else {
            str = String.valueOf(arg1);
        }
        org.apache.spark.sql.catalyst.expressions.SubstringIndex substringIndex = new org.apache.spark.sql.catalyst.expressions.SubstringIndex(null, null, null);
        return substringIndex.nullSafeEval(UTF8String.fromString(str), UTF8String.fromString(arg2), arg3.intValue());
    }
}
