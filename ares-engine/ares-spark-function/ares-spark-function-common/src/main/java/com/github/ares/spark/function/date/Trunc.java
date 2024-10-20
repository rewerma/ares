package com.github.ares.spark.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.unsafe.types.UTF8String;

import java.time.LocalDate;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toLocalDate;


@AutoService(SparkFuncInterface.class)
public class Trunc implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "TRUNC";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return LocalTimeType.LOCAL_DATE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        try {
            Object arg1 = args.get(0);
            Object arg2 = args.get(1);
            if (arg1 == null || arg2 == null) {
                return null;
            }
            LocalDate localDate = toLocalDate(arg1);
            if (localDate == null) {
                return null;
            }
            String levelStr = arg2.toString().toUpperCase();
            int level = DateTimeUtils.parseTruncLevel(UTF8String.fromString(levelStr));
            int res = DateTimeUtils.truncDate((int) localDate.toEpochDay(), level);
            return LocalDate.ofEpochDay(res);
        } catch (Exception e) {
            return null;
        }
    }
}
