package com.github.ares.spark.function.number;

import static com.github.ares.sql.function.utils.FunctionArgumentValid.validateArgCount;
import static com.github.ares.sql.function.utils.Utils.toNumber;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import java.util.List;
import org.apache.spark.sql.catalyst.expressions.Signum;

@AutoService(SparkFuncInterface.class)
public class Sign implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "SIGN";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.DOUBLE_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        validateArgCount(functionName(), 1, args.size());
        Number arg = toNumber(args.get(0));
        if (arg == null) {
            return null;
        }
        Signum signum = new Signum(null);
        return signum.nullSafeEval(arg.doubleValue());
    }
}
