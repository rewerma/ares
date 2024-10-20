package com.github.ares.sql.function;

import com.github.ares.api.table.type.AresDataType;

import java.io.Serializable;
import java.util.List;

public interface FunctionInterface extends Serializable {
    String functionName();

    AresDataType<?> resultType(List<AresDataType<?>> argTypes);

    Object evaluate(List<Object> args);

    static FunctionInterface fromUdf(UdfInterface udf) {
        FunctionInterface function = new FunctionInterface() {
            @Override
            public String functionName() {
                return udf.functionName();
            }

            @Override
            public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
                return udf.resultType();
            }

            @Override
            public Object evaluate(List<Object> args) {
                return udf.evaluate(args);
            }
        };
        return function;
    }
}
