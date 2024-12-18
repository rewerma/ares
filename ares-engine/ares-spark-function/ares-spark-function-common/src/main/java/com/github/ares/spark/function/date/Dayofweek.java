package com.github.ares.spark.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.DayOfMonth;
import org.apache.spark.sql.catalyst.expressions.DayOfWeek;

import java.time.LocalDate;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toLocalDate;


@AutoService(SparkFuncInterface.class)
public class Dayofweek implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "DAYOFWEEK";
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
        LocalDate localDate = toLocalDate(arg);
        if (localDate == null) {
            return null;
        }
        long epochDay = localDate.toEpochDay();
        DayOfWeek dayOfWeek = new DayOfWeek(null);
        return dayOfWeek.nullSafeEval((int) epochDay);
    }
}
