package com.github.ares.spark.function.system;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import java.util.List;

@AutoService(SparkFuncInterface.class)
public class Isnull implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "ISNULL";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.BOOLEAN_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());
        Object arg = args.get(0);
        if (arg == null) {
            return true;
        } else {
            return false;
        }
    }
}
