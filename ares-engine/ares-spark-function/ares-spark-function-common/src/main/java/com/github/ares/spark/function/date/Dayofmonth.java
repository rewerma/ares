package com.github.ares.spark.function.date;

import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;


@AutoService(SparkFuncInterface.class)
public class Dayofmonth extends Day implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "DAYOFMONTH";
    }

}
