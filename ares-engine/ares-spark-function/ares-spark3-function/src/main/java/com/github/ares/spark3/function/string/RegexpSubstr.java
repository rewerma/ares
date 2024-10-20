package com.github.ares.spark3.function.string;

import com.github.ares.spark.function.string.RegexpExtract;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.List;

@AutoService(SparkFuncInterface.class)
public class RegexpSubstr extends RegexpExtract implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "REGEXP_SUBSTR";
    }

    @Override
    public Object evaluate(List<Object> args) {
        List<Object> argList = new ArrayList<>(args);
        argList.add(0);
        String result = (String) super.evaluate(argList);
        if(result != null && result.isEmpty()) {
            return null;
        }
        return result;
    }
}
