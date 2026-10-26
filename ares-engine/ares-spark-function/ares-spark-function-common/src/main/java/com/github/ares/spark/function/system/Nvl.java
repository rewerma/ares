package com.github.ares.spark.function.system;

import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

@AutoService(SparkFuncInterface.class)
public class Nvl extends Ifnull implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "NVL";
    }
}
