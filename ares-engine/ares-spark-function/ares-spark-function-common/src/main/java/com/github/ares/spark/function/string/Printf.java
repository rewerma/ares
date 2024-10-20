package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.util.List;

import static com.github.ares.sql.function.utils.Utils.toStr;

@AutoService(SparkFuncInterface.class)
public class Printf implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "PRINTF";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        if (args.isEmpty()) {
            throw new AresException("The `printf` requires > 0 parameters but the actual number is 0.");
        }
        String arg = toStr(args.get(0));
        if (arg == null) {
            return null;
        }
        Object[] params = new Object[args.size() - 1];
        for (int i = 1; i < args.size(); i++) {
            params[i - 1] = args.get(i);
        }
        return String.format(arg, params);
    }
}
