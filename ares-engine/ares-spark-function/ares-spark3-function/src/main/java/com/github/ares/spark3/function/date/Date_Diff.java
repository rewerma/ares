package com.github.ares.spark3.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.DateDiff;

import java.time.LocalDate;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toLocalDate;


@AutoService(SparkFuncInterface.class)
public class Date_Diff implements SparkFuncInterface {

    @Override
    public String functionName() {
        return "DATE_DIFF";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.LONG_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        LocalDate date1 = toLocalDate(args.get(0));
        LocalDate date2 = toLocalDate(args.get(1));
        if (date1 == null || date2 == null) {
            return null;
        }
        DateDiff dateDiff = new DateDiff(null, null);
        long d1 = date1.toEpochDay();
        long d2 = date2.toEpochDay();
        return dateDiff.nullSafeEval((int) d1, (int) d2);
    }
}
