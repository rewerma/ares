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
public class DateFromUnixDate implements SparkFuncInterface {

    @Override
    public String functionName() {
        return "DATE_FROM_UNIX_DATE";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return LocalTimeType.LOCAL_DATE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());
        try {
            Number arg = toNumber(args.get(0));
            if (arg == null) {
                return null;
            }
            org.apache.spark.sql.catalyst.expressions.DateFromUnixDate dateFromUnixDate = new org.apache.spark.sql.catalyst.expressions.DateFromUnixDate(null);
            Object res = dateFromUnixDate.nullSafeEval(arg.intValue());
            return LocalDate.ofEpochDay((int) res);
        } catch (Exception e) {
            return null;
        }
    }
}
