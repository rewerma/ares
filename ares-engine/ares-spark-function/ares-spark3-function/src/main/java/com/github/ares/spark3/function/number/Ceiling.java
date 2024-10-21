package com.github.ares.spark3.function.number;

import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

@AutoService(SparkFuncInterface.class)
public class Ceiling extends Ceil implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "CEILING";
    }
}