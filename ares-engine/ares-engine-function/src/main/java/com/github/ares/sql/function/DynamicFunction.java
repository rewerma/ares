package com.github.ares.sql.function;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.common.exceptions.AresException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DynamicFunction implements Serializable {

    private String functionName;

    private AresDataType<?> resultType;

    private List<AresDataType<?>> argTypes = new ArrayList<>();
    private Function function;

    public Object evaluate(List<Object> args) {
        return function.evaluate(args);
    }

    public UdfInterface toUdfInterface() {
        DynamicFunction dynamicFunction = this;
        UdfInterface udfInterface = new UdfInterface() {
            @Override
            public String functionName() {
                return dynamicFunction.getFunctionName();
            }

            @Override
            public AresDataType<?> resultType() {
                return dynamicFunction.getResultType();
            }

            @Override
            public List<AresDataType<?>> argTypes() {
                return dynamicFunction.getArgTypes();
            }

            @Override
            public Object evaluate(List<Object> args) {
                return dynamicFunction.evaluate(args);
            }
        };
        return udfInterface;
    }

    public void fromUdfInterface(UdfInterface udfInterface) {
        this.setFunctionName(udfInterface.functionName());
        this.setResultType(udfInterface.resultType());
        if (udfInterface.argTypes() != null) {
            this.setArgTypes(udfInterface.argTypes());
        }
        this.setFunction(udfInterface::evaluate);
    }

    public Function getFunction() {
        return function;
    }

    public void setFunction(Function function) {
        this.function = function;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public AresDataType<?> getResultType() {
        return resultType;
    }

    public void setResultType(AresDataType<?> resultType) {
        this.resultType = resultType;
    }

    public List<AresDataType<?>> getArgTypes() {
        return argTypes;
    }

    public void setArgTypes(List<AresDataType<?>> argTypes) {
        this.argTypes = argTypes;
    }
}
