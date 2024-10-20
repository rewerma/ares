package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class Locate implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "LOCATE";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.INT_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), new int[]{2, 3}, args.size());

        String search = toStr(args.get(0));
        String s = toStr(args.get(1));
        if (s == null || search == null) {
            return null;
        }
        int start = 1;
        if (args.size() == 3) {
            start = ((Number) args.get(2)).intValue();
        }
        if (start < 1) {
            return 0;
        }
        return s.indexOf(search, start - 1) + 1;
    }
}
