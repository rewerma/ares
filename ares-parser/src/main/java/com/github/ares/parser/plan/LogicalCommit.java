package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;

import java.io.Serializable;

public class LogicalCommit extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    public LogicalCommit() {
        super(OperationType.COMMIT);
    }
}
