package com.github.ares.spark3.function.number;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Locale;
import org.apache.spark.sql.catalyst.util.ToNumberParser;
import org.apache.spark.unsafe.types.UTF8String;

@AutoService(SparkFuncInterface.class)
public class ToNumber implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "TO_NUMBER";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.DOUBLE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());

        String arg1 = toStr(args.get(0));
        String arg2 = toStr(args.get(1));
        if (arg1 == null || arg2 == null) {
            return null;
        }
        ToNumberParser toNumberParser = new ToNumberParser(arg2.toUpperCase(Locale.ROOT), true);
        return toNumberParser.parse(UTF8String.fromString(arg1)).toBigDecimal().bigDecimal();
    }
}
