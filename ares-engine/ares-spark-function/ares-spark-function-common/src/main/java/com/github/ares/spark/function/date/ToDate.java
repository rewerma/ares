package com.github.ares.spark.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static com.github.ares.common.utils.DateTimeUtils.stringToLocalDateTime;
import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class ToDate implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "TO_DATE";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return LocalTimeType.LOCAL_DATE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{1, 2}, args.size());
        String str = toStr(args.get(0));
        if (str == null) {
            return null;
        }
        if (args.size() == 1) {
            try {
                return stringToLocalDateTime(str).toLocalDate();
            } catch (Exception e) {
                return null;
            }
        } else {
            String format = toStr(args.get(1));
            if (format == null) {
                return null;
            }
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
            return LocalDate.parse(str, formatter);
        }
    }
}
