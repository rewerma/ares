package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import lombok.Data;

import java.io.Serializable;

@Data
public abstract class LogicalOperation implements Serializable {
    private static final long serialVersionUID = 1L;

    private OperationType operationType;

    public LogicalOperation(OperationType operationType) {
        this.operationType = operationType;
    }

}
