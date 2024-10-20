package com.github.ares.spark.function.string;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.sql.function.SparkFuncInterface;
import com.google.auto.service.AutoService;
import org.apache.spark.sql.catalyst.util.RandomUUIDGenerator;

import java.util.List;

@AutoService(SparkFuncInterface.class)
public class Uuid implements SparkFuncInterface {
    @Override
    public String functionName() {
        return "UUID";
    }

    @Override
    public AresDataType<?> resultType(List<AresDataType<?>> argTypes) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        RandomUUIDGenerator randomUUIDGenerator = new RandomUUIDGenerator(0);
        return randomUUIDGenerator.getNextUUIDUTF8String().toString();
    }
}
