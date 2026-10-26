package com.github.ares.spark.function.number;

import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import java.math.RoundingMode;
import java.util.List;

@AutoService(SparkFuncInterface.class)
public class Floor extends Round implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "FLOOR";
    }

    @Override
    public Object evaluate(List<Object> args) {
        return evaluate(args, RoundingMode.FLOOR);
    }
}
