package com.github.ares.spark.function.system;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.List;

import static com.github.ares.sql.function.utils.Utils.toNumber;

@AutoService(SparkFuncInterface.class)
public class Elt implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "ELT";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.ANY_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        if (args.size() < 2) {
            throw new AresException(
                    String.format(
                            "The `elt` requires > 1 parameters but the actual number is %d", args.size()));
        }
        Number arg = toNumber(args.get(0));
        if (arg == null) {
            return null;
        }
        List<Object> data = new ArrayList<>();
        for (int i = 1; i < args.size(); i++) {
            data.add(args.get(i));
        }
        int index = arg.intValue();
        if (index > data.size()) {
            return null;
        }

        return data.get(index - 1);
    }
}
