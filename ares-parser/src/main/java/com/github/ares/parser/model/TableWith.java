package com.github.ares.parser.model;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalOperation;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

@Getter
@Setter
public abstract class TableWith extends LogicalOperation implements Serializable {
    protected String connector;

    protected String tableName;

    protected Map<String, Object> options = new LinkedHashMap<>();

    public TableWith(OperationType plainType) {
        super(plainType);
    }
}
