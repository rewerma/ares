package com.github.ares.spark.function.string;

import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

@AutoService(SparkFuncInterface.class)
public class Btrim extends Trim implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "BTRIM";
    }

}
