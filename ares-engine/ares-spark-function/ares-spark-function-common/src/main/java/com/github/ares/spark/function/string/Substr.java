package com.github.ares.spark.function.string;


import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

@AutoService(SparkFuncInterface.class)
public class Substr extends Substring implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "SUBSTR";
    }
}
