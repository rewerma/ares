package com.github.ares.spark.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UnaryPositive;
import org.apache.spark.sql.types.Decimal;
import scala.Tuple2;

import java.util.List;

import static com.github.ares.spark.function.utils.TypeUtil.getNumberType;
import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;


@AutoService(SparkFuncInterface.class)
public class Positive implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "POSITIVE";
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
        UnaryPositive positive = new UnaryPositive(null);
        return positive.nullSafeEval(arg);
    }
}
