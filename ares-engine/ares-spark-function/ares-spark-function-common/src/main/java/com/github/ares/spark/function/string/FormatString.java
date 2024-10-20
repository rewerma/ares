package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Formatter;
import java.util.List;
import java.util.Locale;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class FormatString implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "FORMAT_STRING";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        if (args.isEmpty()) {
            throw new AresException(
                    String.format(
                            "The `format_string` requires > 0 parameters but the actual number is %d", args.size()));
        }

        StringBuffer sb = new StringBuffer();
        Formatter formatter = new java.util.Formatter(sb, Locale.US);

        String pattern = toStr(args.get(0));
        Object[] argList;
        if (args.size() > 1) {
            argList = args.subList(1, args.size()).toArray();
        } else {
            argList = new Object[0];
        }
        formatter.format(pattern, argList);
        return sb.toString();
    }
}
