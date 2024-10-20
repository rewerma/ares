package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(SparkFuncInterface.class)
public class ConcatWs implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "CONCAT_WS";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        int i = 1;
        String separator = (String) args.get(0);
        StringBuilder builder = new StringBuilder();
        boolean f = false;
        for (int l = args.size(); i < l; i++) {
            Object arg = args.get(i);
            if (arg == null) {
                continue;
            }
            if (separator != null) {
                if (f) {
                    builder.append(separator);
                }
                f = true;
            }
            builder.append(arg);
        }
        return builder.toString();
    }
}
