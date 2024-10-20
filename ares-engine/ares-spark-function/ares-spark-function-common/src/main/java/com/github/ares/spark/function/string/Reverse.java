package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.spark.function.utils.ArrayTypeExpression;
import com.github.ares.spark.function.utils.BinaryTypeExpression;
import com.github.ares.spark.function.utils.StringTypeExpression;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class Reverse implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "REVERSE";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return argTypes.get(0);
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());

        Object arg = args.get(0);
        if (arg == null) {
            return null;
        }
        if (arg instanceof byte[]) {
            org.apache.spark.sql.catalyst.expressions.Reverse reverse = new org.apache.spark.sql.catalyst.expressions.Reverse(new ArrayTypeExpression());
            ArrayData arrayData = new GenericArrayData((byte[]) arg);
            ArrayData result = (ArrayData) reverse.nullSafeEval(arrayData);
            Object[] objects = result.array();
            byte[] bytes = new byte[objects.length];
            for (int i = 0; i < objects.length; i++) {
                bytes[i] = (byte) objects[i];
            }
            return bytes;
        } else {
            org.apache.spark.sql.catalyst.expressions.Reverse reverse = new org.apache.spark.sql.catalyst.expressions.Reverse(new StringTypeExpression());
            return reverse.nullSafeEval(UTF8String.fromString(arg.toString())).toString();
        }
    }
}
