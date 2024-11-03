package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;


import java.io.Serializable;

public class LogicalExitLoop extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    public LogicalExitLoop() {
        super(OperationType.EXIT_LOOP);
    }
}
