package com.github.ares.spark3.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.spark.function.utils.TypeUtil;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.BitwiseCount;
import org.apache.spark.sql.catalyst.expressions.Expression;
import scala.Tuple2;

import java.util.List;

import static com.github.ares.spark.function.utils.TypeUtil.handleStringType;
import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;

@AutoService(SparkFuncInterface.class)
public class BitCount implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "BIT_COUNT";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.INT_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());

        Object arg = args.get(0);
        if (arg == null) {
            return null;
        }
        if (!(arg instanceof Integer) && !(arg instanceof Long) && !(arg instanceof Short) && !(arg instanceof Byte)) {
            throw new AresException("cannot resolve 'bit_count(" + handleStringType(args.get(0)) + ")' due to data type mismatch: argument 1 requires " +
                    "integral type, however, " + handleStringType(args.get(0)) + " is of " + args.get(0).getClass().getSimpleName() + " type.");
        }

        Tuple2<Expression, Object> tuple2 = TypeUtil.getNumberType(arg, null);
        BitwiseCount bitwiseCount = new BitwiseCount(tuple2._1());
        return bitwiseCount.nullSafeEval(tuple2._2());

    }
}
