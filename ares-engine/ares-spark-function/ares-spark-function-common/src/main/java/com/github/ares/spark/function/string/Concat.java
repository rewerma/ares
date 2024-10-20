package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(SparkFuncInterface.class)
public class Concat implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "CONCAT";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        int i = 0;
        StringBuilder builder = new StringBuilder();
        for (int l = args.size(); i < l; i++) {
            Object v = args.get(i);
            if (v == null) {
                return null;
            }
            builder.append(v);
        }
        return builder.toString();
    }
}
