package com.github.ares.spark3.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.EndsWith;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class Mask implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "MASK";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{1, 2, 3, 4, 5}, args.size());
        String arg1 = toStr(args.get(0));
        if (arg1 == null) {
            return null;
        }
        String maskUpper = "X";
        if (args.size() > 1) {
            maskUpper = toStr(args.get(1));
        }
        String maskLower = "x";
        if (args.size() > 2) {
            maskLower = toStr(args.get(2));
        }
        String maskDigit = "n";
        if (args.size() > 3) {
            maskDigit = toStr(args.get(3));
        }
        String maskOther = null;
        if (args.size() > 4) {
            maskOther = toStr(args.get(4));
        }

        return org.apache.spark.sql.catalyst.expressions.Mask.transformInput(UTF8String.fromString(arg1),
                maskUpper == null ? null : UTF8String.fromString(maskUpper), maskLower == null ? null : UTF8String.fromString(maskLower),
                maskDigit == null ? null : UTF8String.fromString(maskDigit), maskOther == null ? null : UTF8String.fromString(maskOther)).toString();
    }
}
