package com.github.ares.spark.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;

@AutoService(SparkFuncInterface.class)
public class CurrentTimestamp implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "CURRENT_TIMESTAMP";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return LocalTimeType.LOCAL_DATE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 0, args.size());
        return LocalDateTime.now();
    }
}
