package com.github.ares.spark3.function.system;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(SparkFuncInterface.class)
public class RaiseError implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "RAISE_ERROR";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        if (args.size() != 1) {
            throw new AresException("The `raise_error` requires 1 parameters but the actual number is " + args.size() + ".");
        }
        String message = String.valueOf(args.get(0));
        throw new RuntimeException(message);
    }
}
