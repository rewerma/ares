package com.github.ares.spark.function.date;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.DayOfMonth;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toLocalDate;
import static com.github.ares.sql.function.utils.Utils.toLocalDateTime;


@AutoService(SparkFuncInterface.class)
public class Hour implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "HOUR";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.INT_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());
        Object arg = args.get(0);
        if (arg == null) {
            return null;
        }
        LocalDateTime localDateTime = toLocalDateTime(arg);
        if (localDateTime == null) {
            return null;
        }
        return localDateTime.getHour();
    }
}
