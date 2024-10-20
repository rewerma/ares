package com.github.ares.spark3.function.string;

import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

@AutoService(SparkFuncInterface.class)
public class RegexpLike extends Regexp implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "REGEXP_LIKE";
    }
}
