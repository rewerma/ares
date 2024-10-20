package com.github.ares.spark3.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.unsafe.types.UTF8String;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toLocalDateTime;

@AutoService(SparkFuncInterface.class)
public class ConvertTimezone implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "CONVERT_TIMEZONE";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{2, 3}, args.size());
        Object arg = args.get(0);
        Object arg1 = args.get(1);

        if (arg == null || arg1 == null) {
            return null;
        }
        UTF8String srcTimeZone;
        UTF8String targetTimeZone;
        LocalDateTime dateTime;
        if (args.size() == 2) {
            ZoneId zoneId = ZoneId.systemDefault();
            srcTimeZone = UTF8String.fromString(zoneId.getId());
            targetTimeZone = UTF8String.fromString(String.valueOf(arg));
            dateTime = toLocalDateTime(arg1);
        } else {
            Object arg2 = args.get(2);
            if (arg2 == null) {
                return null;
            }
            srcTimeZone = UTF8String.fromString(String.valueOf(arg));
            targetTimeZone = UTF8String.fromString(String.valueOf(arg1));
            dateTime = toLocalDateTime(arg2);
        }
        if (dateTime == null) {
            return null;
        }

        long micros = DateTimeUtils.localDateTimeToMicros(dateTime);
        org.apache.spark.sql.catalyst.expressions.ConvertTimezone convertTimezone = new org.apache.spark.sql.catalyst.expressions.ConvertTimezone(null, null, null);
        Object res = convertTimezone.nullSafeEval(srcTimeZone, targetTimeZone, micros);
        return DateTimeUtils.microsToLocalDateTime((Long) res);
    }
}
