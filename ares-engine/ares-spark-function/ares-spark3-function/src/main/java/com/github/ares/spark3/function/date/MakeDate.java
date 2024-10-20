package com.github.ares.spark3.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.time.LocalDate;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;


@AutoService(SparkFuncInterface.class)
public class MakeDate implements SparkFuncInterface {

    @Override
    public String functionName() {
        return "MAKE_DATE";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return LocalTimeType.LOCAL_DATE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 3, args.size());
        Number arg1 = toNumber(args.get(0));
        Number arg2 = toNumber(args.get(1));
        Number arg3 = toNumber(args.get(2));
        if (arg1 == null || arg2 == null || arg3 == null) {
            return null;
        }
        org.apache.spark.sql.catalyst.expressions.MakeDate makeDate = new org.apache.spark.sql.catalyst.expressions.MakeDate(null, null, null);
        Object res = makeDate.nullSafeEval(arg1.intValue(), arg2.intValue(), arg3.intValue());
        return LocalDate.ofEpochDay((Integer) res);
    }
}
