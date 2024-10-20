package com.github.ares.sql.function;

import com.github.ares.api.table.type.AresDataType;

import java.io.Serializable;
import java.util.List;

public interface UdfInterface extends Serializable {

    String functionName();

    AresDataType<?> resultType();

    List<AresDataType<?>> argTypes();

    Object evaluate(List<Object> args);
}
