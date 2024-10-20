package com.github.ares.spark3.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.spark.function.date.Extract;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.time.LocalDateTime;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toLocalDateTime;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class Date_Part extends Extract implements SparkFuncInterface {

    @Override
    public String functionName() {
        return "DATE_PART";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.DOUBLE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        List<Object> argsTmp = new ArrayList<>();
        argsTmp.add(args.get(1));
        argsTmp.add(args.get(0));
        return super.evaluate(argsTmp);
    }
}
