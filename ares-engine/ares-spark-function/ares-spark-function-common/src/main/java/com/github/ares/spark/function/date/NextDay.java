package com.github.ares.spark.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.unsafe.types.UTF8String;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static com.github.ares.common.utils.DateTimeUtils.stringToLocalDateTime;
import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class NextDay implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "NEXT_DAY";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return LocalTimeType.LOCAL_DATE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        Object arg1 = args.get(0);
        String arg2 = toStr(args.get(1));
        if (arg1 == null || arg2 == null) {
            return null;
        }
        LocalDate localDate;
        if (arg1 instanceof LocalDate) {
            localDate = (LocalDate) arg1;
        } else if (arg1 instanceof LocalDateTime) {
            localDate = ((LocalDateTime) arg1).toLocalDate();
        } else if (arg1 instanceof String) {
            localDate = stringToLocalDateTime(arg1.toString()).toLocalDate();
        } else {
            throw new AresException("Cannot resolve \"next_day(" + arg1 + ", " + arg2 + ")\" due to data type mismatch: " +
                    "Parameter 1 requires the \"DATE\" type, however \"" + arg1 + "\" has the type \"" + arg1.getClass().getSimpleName() + "\".");
        }

        org.apache.spark.sql.catalyst.expressions.NextDay nextDay = new org.apache.spark.sql.catalyst.expressions.NextDay(null, null);
        Object res = nextDay.nullSafeEval((int) localDate.toEpochDay(), UTF8String.fromString(arg2));
        if (res != null) {
            return LocalDate.ofEpochDay((Integer) res);
        }
        return null;
    }
}
