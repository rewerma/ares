package com.github.ares.spark.function.number;

import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

@AutoService(SparkFuncInterface.class)
public class Power extends Pow implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "POWER";
    }
}
