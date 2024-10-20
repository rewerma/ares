package com.github.ares.spark.function.system;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;

@AutoService(SparkFuncInterface.class)
public class Ifnull implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "IFNULL";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.ANY_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        Object arg1 = args.get(0);
        Object arg2 = args.get(1);
        if (arg1 == null) {
            return arg2;
        } else {
            return arg1;
        }
    }
}
