package com.github.ares.spark.function.system;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class FindInSet implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "FIND_IN_SET";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.ANY_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        String arg1 = toStr(args.get(0));
        String arg2 = toStr(args.get(1));
        if (arg1 == null && arg2 == null) {
            return null;
        }
        org.apache.spark.sql.catalyst.expressions.FindInSet findInSet = new org.apache.spark.sql.catalyst.expressions.FindInSet(null, null);
        return findInSet.nullSafeEval(UTF8String.fromString(arg1), UTF8String.fromString(arg2));
    }
}
