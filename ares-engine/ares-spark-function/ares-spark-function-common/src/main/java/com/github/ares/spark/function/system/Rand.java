package com.github.ares.spark.function.system;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.util.random.XORShiftRandom;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;

@AutoService(SparkFuncInterface.class)
public class Rand implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "RAND";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.DOUBLE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{0, 1}, args.size());
        Long seed = null;
        if (args.size() == 1 && args.get(0)!= null) {
            Object arg = args.get(0);
            if (!(arg instanceof Long) && !(arg instanceof Integer) && !(arg instanceof Short) && !(arg instanceof Byte)) {
                throw new AresException("Cannot resolve \"" + functionName() + "(" + arg + ")\" due to data type mismatch: " +
                        "Parameter 1 requires the (\"INT\" or \"BIGINT\") type, however \"" + arg + "\" has the type \"" + arg.getClass().getSimpleName() + "\".");
            }
            seed = ((Number) arg).longValue();
        }
        XORShiftRandom rd;
        if (seed != null) {
            rd = new XORShiftRandom(seed);
        } else {
            rd = new XORShiftRandom();
        }
        return rd.nextDouble();
    }
}
