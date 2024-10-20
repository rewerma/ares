package com.github.ares.engine.core;

import com.github.ares.parser.model.CreateMethod;

import java.io.Serializable;
import java.util.List;

public interface CreateProcedureFunc extends Serializable {
    default String functionName() {
        throw new UnsupportedOperationException("Not implemented");
    }

    List<Serializable> evaluate(List<Serializable> args);

    default CreateMethod getCreateMethod() {
        return null;
    }
}
