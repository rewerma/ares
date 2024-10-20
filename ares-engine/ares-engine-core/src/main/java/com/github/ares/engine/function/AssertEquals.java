package com.github.ares.engine.function;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.UdfInterface;
import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@AutoService(UdfInterface.class)
public class AssertEquals implements UdfInterface {
    @Override
    public String functionName() {
        return "ASSERT_EQUALS";
    }

    @Override
    public AresDataType<?> resultType() {
        return BasicType.STRING_TYPE;
    }

    @Override
    public List<AresDataType<?>> argTypes() {
        return Arrays.asList(BasicType.ANY_TYPE, BasicType.ANY_TYPE);
    }

    @Override
    public Object evaluate(List<Object> args) {
        if (args.size() != 2) {
            throw new AresException("ASSERT function expects two argument");
        }
        Object arg1 = args.get(0);
        System.out.println(arg1);
        Object arg2 = args.get(1);
        if (arg1 == null && arg2 == null) {
            return arg1;
        }
        if (Objects.equals(arg1, arg2)) {
            return arg1;
        } else {
            if (arg1 instanceof String) {
                arg1 = "'" + arg1 + "'";
            }
            if (arg2 instanceof String) {
                arg2 = "'" + arg2 + "'";
            }
            throw new AresException("ASSERT_EQUAL failed: " + arg1 + " != " + arg2);
        }
    }
}
