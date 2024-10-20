package com.github.ares.engine.function;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.UdfInterface;
import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.List;

import static com.github.ares.common.utils.StringUtils.println;

@AutoService(UdfInterface.class)
public class PutLineFunction implements UdfInterface {

    @Override
    public String functionName() {
        return "put_line";
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
            throw new IllegalArgumentException("put_line function expects one argument");
        }
        println(args.get(0));
        return args.get(0);
    }
}