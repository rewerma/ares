package com.github.ares.spark.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
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
import org.apache.spark.sql.types.Decimal;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.List;

import static com.github.ares.spark.function.utils.TypeUtil.getNumberType;
import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;


@AutoService(SparkFuncInterface.class)
public class Abs implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "ABS";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        if (argTypes.get(0) == BasicType.STRING_TYPE) {
            return BasicType.DOUBLE_TYPE;
        }
        return argTypes.get(0);
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());
        Object arg = toNumber(args.get(0));
        if (arg == null) {
            return null;
        }
        Tuple2<Expression, Object> tuple = getNumberType(arg);
        org.apache.spark.sql.catalyst.expressions.Abs abs = new org.apache.spark.sql.catalyst.expressions.Abs(tuple._1());
        arg = tuple._2();
        Object result = abs.nullSafeEval(arg);
        if (result == null) {
            return null;
        }
        if (result instanceof Decimal) {
            return ((Decimal) result).toBigDecimal().bigDecimal();
        }
        return result;
    }
}
