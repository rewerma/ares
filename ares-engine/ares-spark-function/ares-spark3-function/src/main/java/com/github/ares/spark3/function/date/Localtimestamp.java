package com.github.ares.spark3.function.date;

import com.github.ares.spark.function.date.CurrentTimestamp;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

@AutoService(SparkFuncInterface.class)
public class Localtimestamp extends CurrentTimestamp implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "LOCALTIMESTAMP";
    }

}
