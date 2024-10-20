package com.github.ares.spark.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static com.github.ares.common.utils.DateTimeUtils.getLocalDateTime;
import static com.github.ares.common.utils.DateTimeUtils.stringToLocalDateTime;
import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class ToTimestamp implements SparkFuncInterface {

    @Override
    public String functionName() {
        return "TO_TIMESTAMP";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{1, 2}, args.size());
        if (args.get(0) == null) {
            return null;
        }

        if (args.size() == 1) {
            try {
                if (args.get(0) instanceof String) {
                    return stringToLocalDateTime((String) args.get(0));
                } else if (args.get(0) instanceof Number) {
                    long epochSecond = ((Number) args.get(0)).longValue();
                    return getLocalDateTime(epochSecond * 1000000L, ZoneId.systemDefault());
                } else if (args.get(0) instanceof LocalDateTime) {
                    return args.get(0);
                } else if (args.get(0) instanceof LocalDate) {
                    return ((LocalDate) args.get(0)).atStartOfDay();
                }
                return null;
            } catch (Exception e) {
                return null;
            }
        } else {
            String format = toStr(args.get(1));
            if (format == null) {
                return null;
            }
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
            return LocalDateTime.parse(toStr(args.get(0)), formatter);
        }
    }

}
