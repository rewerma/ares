package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;

import java.io.Serializable;

public class LogicalContinueLoop extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = 1L;

    public LogicalContinueLoop() {
        super(OperationType.CONTINUE_LOOP);
    }
}
