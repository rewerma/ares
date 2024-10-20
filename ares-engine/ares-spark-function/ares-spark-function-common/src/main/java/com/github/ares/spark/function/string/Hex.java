package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.spark.function.utils.BinaryTypeExpression;
import com.github.ares.spark.function.utils.LongTypeExpression;
import com.github.ares.spark.function.utils.StringTypeExpression;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;

@AutoService(SparkFuncInterface.class)
public class Hex implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "HEX";
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
        org.apache.spark.sql.catalyst.expressions.Hex hex;
        if (arg instanceof byte[]) {
            hex = new org.apache.spark.sql.catalyst.expressions.Hex(new BinaryTypeExpression());
        } else if (arg instanceof Number) {
            arg = ((Number) arg).longValue();
            hex = new org.apache.spark.sql.catalyst.expressions.Hex(new LongTypeExpression());
        } else {
            arg = UTF8String.fromString(arg.toString());
            hex = new org.apache.spark.sql.catalyst.expressions.Hex(new StringTypeExpression());
        }
        return hex.nullSafeEval(arg).toString();
    }
}
