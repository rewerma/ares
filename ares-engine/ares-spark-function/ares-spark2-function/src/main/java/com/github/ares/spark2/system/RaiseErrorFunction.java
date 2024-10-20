package com.github.ares.spark2.system;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.UdfInterface;
import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@AutoService(UdfInterface.class)
public class RaiseErrorFunction implements UdfInterface {
    @Override
    public String functionName() {
        return "raise_error";
    }

    @Override
    public AresDataType<?> resultType() {
        return BasicType.STRING_TYPE;
    }

    @Override
    public List<AresDataType<?>> argTypes() {
        return Collections.singletonList(BasicType.STRING_TYPE);
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
