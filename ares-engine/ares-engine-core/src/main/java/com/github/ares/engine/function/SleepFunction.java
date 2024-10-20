package com.github.ares.engine.function;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.UdfInterface;
import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.List;

@AutoService(UdfInterface.class)
public class SleepFunction implements UdfInterface {

    @Override
    public String functionName() {
        return "sleep";
    }

    @Override
    public AresDataType<?> resultType() {
        return BasicType.LONG_TYPE;
    }

    @Override
    public List<AresDataType<?>> argTypes() {
        return Collections.singletonList(BasicType.LONG_TYPE);
    }

    @Override
    public Object evaluate(List<Object> args) {
        if (args.size() != 1) {
            throw new IllegalArgumentException("sleep function expects one argument");
        }
        long time = 0;
        try {
            time = Long.parseLong(args.get(0).toString());
            Thread.sleep(time * 1000L);
        } catch (Exception e) {
            throw new IllegalArgumentException("sleep function expects a long value as argument");
        }
        return time;
    }
}