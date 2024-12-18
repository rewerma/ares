package com.github.ares.spark.function.system;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;

@AutoService(SparkFuncInterface.class)
public class If implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "IF";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.ANY_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 3, args.size());
        Object arg1 = args.get(0);
        if (arg1 == null) {
            arg1 = Boolean.FALSE;
        }
        Object arg2 = args.get(1);
        Object arg3 = args.get(2);
        if (!(arg1 instanceof Boolean)) {
            throw new AresException("Cannot resolve \"IF(" + arg1 + ", " + arg2 + ", " + arg3 + ")\" due to data type mismatch: " +
                    "Parameter 1 requires the \"BOOLEAN\" type, however \"" + arg1 + "\" has the type \"" + arg1.getClass().getSimpleName() + "\".");
        }
        if ((Boolean) arg1) {
            return arg2;
        } else {
            return arg3;
        }
    }
}
