package com.github.ares.spark.function.system;

import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

@AutoService(SparkFuncInterface.class)
public class Random extends Rand implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "RANDOM";
    }
}
