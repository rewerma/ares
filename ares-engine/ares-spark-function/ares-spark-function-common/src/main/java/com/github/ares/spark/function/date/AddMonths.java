package com.github.ares.spark.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.time.LocalDate;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toLocalDate;
import static com.github.ares.sql.function.utils.Utils.toNumber;

@AutoService(SparkFuncInterface.class)
public class AddMonths implements SparkFuncInterface {

    @Override
    public String functionName() {
        return "ADD_MONTHS";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return LocalTimeType.LOCAL_DATE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        try {
            Object dateObj = args.get(0);
            if (dateObj == null) {
                return null;
            }
            int months = toNumber(args.get(1)).intValue();
            LocalDate date = toLocalDate(dateObj);
            if (date == null) {
                return null;
            }
            org.apache.spark.sql.catalyst.expressions.AddMonths addMonths = new org.apache.spark.sql.catalyst.expressions.AddMonths(null, null);
            long epochDay = date.toEpochDay();
            int resEpochDay = (int) addMonths.nullSafeEval((int) epochDay, months);
            return LocalDate.ofEpochDay(resEpochDay);
        } catch (Exception e) {
            return null;
        }
    }
}
