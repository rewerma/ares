package com.github.ares.spark.function.string;


import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.spark.function.utils.BinaryTypeExpression;
import com.github.ares.spark.function.utils.StringTypeExpression;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;

@AutoService(SparkFuncInterface.class)
public class Substring implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "SUBSTRING";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{2, 3}, args.size());
        Object arg1 = args.get(0);
        Number arg2 = toNumber(args.get(1));
        if (arg1 == null || arg2 == null) {
            return null;
        }

        org.apache.spark.sql.catalyst.expressions.Substring substring;
        if (arg1 instanceof byte[]) {
            int len = ((byte[]) arg1).length;
            if (args.size() == 3) {
                len = toNumber(args.get(2)).intValue();
            }
            substring = new org.apache.spark.sql.catalyst.expressions.Substring(new BinaryTypeExpression(), null, null);
            return substring.nullSafeEval(arg1, arg2.intValue(), len);
        } else {
            int len = arg1.toString().length();
            if (args.size() == 3) {
                len = toNumber(args.get(2)).intValue();
            }
            substring = new org.apache.spark.sql.catalyst.expressions.Substring(new StringTypeExpression(), null, null);
            return substring.nullSafeEval(UTF8String.fromString(arg1.toString()), arg2.intValue(), len).toString();
        }
    }
}
