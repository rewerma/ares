package com.github.ares.spark2.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.unsafe.types.UTF8String;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.TimeZone;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toLocalDateTime;


@AutoService(SparkFuncInterface.class)
public class DateTrunc implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "DATE_TRUNC";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        try {
            Object arg = args.get(0);
            Object arg1 = args.get(1);
            if (arg == null || arg1 == null) {
                return null;
            }
            LocalDateTime localDateTime = toLocalDateTime(arg1);
            if (localDateTime == null) {
                return null;
            }
            String levelStr = arg.toString().toUpperCase();
            int level = DateTimeUtils.parseTruncLevel(UTF8String.fromString(levelStr));
            long micros = com.github.ares.common.utils.DateTimeUtils.localDateTimeToMicros(localDateTime);
            TimeZone timeZone = TimeZone.getDefault();
            long res = DateTimeUtils.truncTimestamp(micros, level, timeZone);
            return com.github.ares.common.utils.DateTimeUtils.microsToLocalDateTime(res);
        } catch (Exception e) {
            return null;
        }
    }
}
