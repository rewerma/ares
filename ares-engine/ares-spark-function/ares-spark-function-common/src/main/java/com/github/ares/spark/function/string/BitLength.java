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

@AutoService(SparkFuncInterface.class)
public class BitLength implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "BIT_LENGTH";
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
        if (arg instanceof byte[]) {
            org.apache.spark.sql.catalyst.expressions.BitLength bitLength = new org.apache.spark.sql.catalyst.expressions.BitLength(new BinaryTypeExpression());
            return bitLength.nullSafeEval(arg);
        } else {
            org.apache.spark.sql.catalyst.expressions.BitLength bitLength = new org.apache.spark.sql.catalyst.expressions.BitLength(new StringTypeExpression());
            UTF8String utf8String = UTF8String.fromString(arg.toString());
            return bitLength.nullSafeEval(utf8String);
        }
    }
}
