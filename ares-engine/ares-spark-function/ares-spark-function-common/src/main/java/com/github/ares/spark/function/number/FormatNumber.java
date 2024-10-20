package com.github.ares.spark.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.spark.function.utils.DoubleTypeExpression;
import com.github.ares.spark.function.utils.IntegerTypeExpression;
import com.github.ares.spark.function.utils.StringTypeExpression;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;


@AutoService(SparkFuncInterface.class)
public class FormatNumber implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "FORMAT_NUMBER";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        Number arg0 = toNumber(args.get(0));
        Object arg1 = args.get(1);
        if (arg0 == null || arg1 == null) {
            return null;
        }
        org.apache.spark.sql.catalyst.expressions.FormatNumber formatNumber;
        if (arg1 instanceof String) {
            formatNumber = new org.apache.spark.sql.catalyst.expressions.FormatNumber(new DoubleTypeExpression(), new StringTypeExpression());
            return formatNumber.nullSafeEval(arg0.doubleValue(), UTF8String.fromString(arg1.toString())).toString();
        } else if (arg1 instanceof Number) {
            formatNumber = new org.apache.spark.sql.catalyst.expressions.FormatNumber(new DoubleTypeExpression(), new IntegerTypeExpression());
            return formatNumber.nullSafeEval(arg0.doubleValue(), ((Number) arg1).intValue()).toString();
        } else {
            throw new AresException("Cannot resolve \"format_number(" + arg0 + ", " + arg1 + ")\" due to data type mismatch: " +
                    "Parameter 2 requires the (\"INT\" or \"STRING\") type, however \"" + arg1 + "\" has the type \"" + arg1.getClass().getSimpleName() + "\".");
        }
    }
}
