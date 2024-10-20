package com.github.ares.spark.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.spark.function.utils.ByteTypeExpression;
import com.github.ares.spark.function.utils.DecimalTypeExpression;
import com.github.ares.spark.function.utils.DoubleTypeExpression;
import com.github.ares.spark.function.utils.FloatTypeExpression;
import com.github.ares.spark.function.utils.IntegerTypeExpression;
import com.github.ares.spark.function.utils.LongTypeExpression;
import com.github.ares.spark.function.utils.ShortTypeExpression;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UnaryMinus;

import java.math.BigDecimal;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;

@AutoService(SparkFuncInterface.class)
public class Negative implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "NEGATIVE";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.ANY_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());
        Number arg = toNumber(args.get(0));
        if (arg == null) {
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
        } else if (arg instanceof Double) {
            typeExpression = new DoubleTypeExpression();
        } else if (arg instanceof Float) {
            typeExpression = new FloatTypeExpression();
        } else if (arg instanceof BigDecimal) {
            typeExpression = new DecimalTypeExpression();
        } else {
            throw new AresException("Cannot resolve \"negative(" + arg + ")\" due to data type mismatch: " +
                    "Parameter 1 requires the \"NUMBER\" type, however \"" + arg + "\" has the type \"" + arg.getClass().getSimpleName() + "\".");
        }
        UnaryMinus unaryMinus = new UnaryMinus(typeExpression);
        return unaryMinus.nullSafeEval(arg);
    }
}
