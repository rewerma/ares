package com.github.ares.spark3.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.RLike;
import org.apache.spark.sql.catalyst.expressions.RegExpInStr;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class RegexpInstr implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "REGEXP_INSTR";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.INT_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        String arg1 = toStr(args.get(0));
        String arg2 = toStr(args.get(1));
        if (arg1 == null || arg2 == null) {
            return null;
        }
        RegExpInStr regexpInStr = new RegExpInStr(null, null);
        return regexpInStr.nullSafeEval(UTF8String.fromString(arg1), UTF8String.fromString(arg2), 1);
    }
}
