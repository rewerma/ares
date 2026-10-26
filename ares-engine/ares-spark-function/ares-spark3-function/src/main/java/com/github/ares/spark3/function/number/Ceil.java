package com.github.ares.spark3.function.number;

import com.github.ares.spark.function.number.Round;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import java.math.RoundingMode;
import java.util.List;

@AutoService(SparkFuncInterface.class)
public class Ceil extends Round implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "Ceil";
    }

    @Override
    public Object evaluate(List<Object> args) {
        return evaluate(args, RoundingMode.CEILING);
    }
}
