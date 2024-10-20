package com.github.ares.spark.function.system;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(SparkFuncInterface.class)
public class Coalesce implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "COALESCE";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.ANY_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        Object v = null;
        for (Object v2 : args) {
            if (v2 != null) {
                v = v2;
                break;
            }
        }
        return v;
    }
}
