package com.github.ares.spark3.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.spark.function.utils.ByteTypeExpression;
import com.github.ares.spark.function.utils.IntegerTypeExpression;
import com.github.ares.spark.function.utils.LongTypeExpression;
import com.github.ares.spark.function.utils.ShortTypeExpression;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.BitwiseGet;
import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;

@AutoService(SparkFuncInterface.class)
public class Getbit implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "GETBIT";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.BYTE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        Object arg = args.get(0);
        Number arg1 = toNumber(args.get(1));
        if (arg == null || arg1 == null) {
            return null;
        }
        Expression typeExpression = null;
        if (arg instanceof Byte) {
            typeExpression = new ByteTypeExpression();
        } else if (arg instanceof Short) {
            typeExpression = new ShortTypeExpression();
        } else if (arg instanceof Integer) {
            typeExpression = new IntegerTypeExpression();
        } else if (arg instanceof Long) {
            typeExpression = new LongTypeExpression();
        } else {
            throw new AresException("Cannot resolve \"getbit(" + arg + ", " + arg1 + ")\" due to data type mismatch: " +
                    "Parameter 1 requires the \"INTEGRAL\" type, however \"" + arg + "\" has the type \"" + arg.getClass().getSimpleName() + "\".");
        }
        BitwiseGet bitwiseGet = new BitwiseGet(typeExpression, null);
        return bitwiseGet.nullSafeEval(arg, arg1.intValue());
    }
}
