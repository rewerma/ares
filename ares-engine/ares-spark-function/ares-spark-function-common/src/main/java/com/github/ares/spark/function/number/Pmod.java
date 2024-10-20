package com.github.ares.spark.function.number;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresDataTypeHelper;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;


@AutoService(SparkFuncInterface.class)
public class Pmod extends Mod implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "PMOD";
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 2, args.size());
        Number arg1 = toNumber(args.get(0));
        Number arg2 = toNumber(args.get(1));
        if (arg1 == null || arg2 == null) {
            return null;
        }
        Abs abs = new Abs();
        Object absArg1 = abs.evaluate(Collections.singletonList(arg1));
        Object absArg2 = abs.evaluate(Collections.singletonList(arg2));
        if (absArg1 == null || absArg2 == null) {
            return null;
        }
        return super.evaluate(Arrays.asList(absArg1, absArg2));
    }
}
