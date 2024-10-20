package com.github.ares.spark2.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.expressions.RegExpReplace;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class RegexpReplace implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "REGEXP_REPLACE";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 3, args.size());
        String arg1 = toStr(args.get(0));
        String arg2 = toStr(args.get(1));
        String arg3 = toStr(args.get(2));
        if (arg1 == null || arg2 == null || arg3 == null) {
            return null;
        }
        RegExpReplace regexpReplace = new RegExpReplace(null, null, null);
        return regexpReplace.nullSafeEval(UTF8String.fromString(arg1), UTF8String.fromString(arg2), UTF8String.fromString(arg3)).toString();
    }
}
