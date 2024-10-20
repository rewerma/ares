package com.github.ares.engine.function;

import com.github.ares.sql.function.UdfInterface;
import com.google.auto.service.AutoService;

@AutoService(UdfInterface.class)
public class PrintlnFunction extends PutLineFunction implements UdfInterface {

    @Override
    public String functionName() {
        return "println";
    }
}