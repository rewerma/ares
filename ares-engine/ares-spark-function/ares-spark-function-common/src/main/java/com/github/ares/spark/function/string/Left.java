package com.github.ares.spark.function.string;


import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.spark.function.utils.BinaryTypeExpression;
import com.github.ares.spark.function.utils.StringTypeExpression;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.Substring;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class Left implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "LEFT";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        Object arg1 = args.get(0);
        Number arg2 = toNumber(args.get(1));
        if (arg1 == null || arg2 == null) {
            return null;
        }
        Substring substring;
        if (arg1 instanceof byte[]) {
            substring = new Substring(new BinaryTypeExpression(), null, null);
            return substring.nullSafeEval(arg1, 0, arg2.intValue());
        } else {
            substring = new Substring(new StringTypeExpression(), null, null);
            return substring.nullSafeEval(UTF8String.fromString(arg1.toString()), 0, arg2.intValue()).toString();
        }
    }
}