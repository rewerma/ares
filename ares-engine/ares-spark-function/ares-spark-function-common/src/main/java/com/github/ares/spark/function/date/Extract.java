package com.github.ares.spark.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.temporal.WeekFields;
import java.util.List;
import java.util.Locale;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toLocalDateTime;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class Extract implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "EXTRACT";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.INT_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        Object arg = args.get(0);
        String arg1 = toStr(args.get(1));
        if (arg == null || arg1 == null) {
            return null;
        }
        LocalDateTime dateTime = toLocalDateTime(arg);
        if (dateTime == null) {
            return null;
        }

        try {
            switch (arg1.toUpperCase()) {
                case "YEAR":
                    return dateTime.getYear();
                case "MONTH":
                    return dateTime.getMonthValue();
                case "DAY":
                    return dateTime.getDayOfMonth();
                case "HOUR":
                    return dateTime.getHour();
                case "MINUTE":
                    return dateTime.getMinute();
                case "SECOND":
                case "SECONDS":
                    int second = dateTime.getSecond();
                    int millis = dateTime.getNano() / 1000;
                    return Double.parseDouble(second + "." + millis);
                case "WEEK":
                    WeekFields weekFields = WeekFields.of(Locale.getDefault());
                    return dateTime.get(weekFields.weekOfYear());
                case "DAYOFWEEK":
                    DayOfWeek dayOfWeek = dateTime.getDayOfWeek();
                    int dow = dayOfWeek.getValue() + 1;
                    if (dow == 8) {
                        dow = 1;
                    }
                    return dow;
                case "DOY":
                    return dateTime.getDayOfYear();
                case "QUARTER":
                    int month = dateTime.getMonthValue();
                    return (month - 1) / 3 + 1;
                default:
                    return null;
            }
        } catch (Exception e) {
            return null;
        }
    }
}
