package com.github.ares.spark3.function.date;

import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

@AutoService(SparkFuncInterface.class)
public class Datepart extends Date_Part implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "DATEPART";
    }
}
