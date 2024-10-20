package com.github.ares.spark.function.string;


import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.spark.function.utils.BinaryTypeExpression;
import com.github.ares.spark.function.utils.StringTypeExpression;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.StringRepeat;
import org.apache.spark.sql.catalyst.expressions.Substring;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class Repeat implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "REPEAT";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        String arg1 = toStr(args.get(0));
        Number arg2 = toNumber(args.get(1));
        if (arg1 == null || arg2 == null) {
            return null;
        }
        StringRepeat repeat = new StringRepeat(null, null);
        return repeat.nullSafeEval(UTF8String.fromString(arg1), arg2.intValue()).toString();
    }
}
