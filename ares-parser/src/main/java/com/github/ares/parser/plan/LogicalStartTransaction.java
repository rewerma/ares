package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import java.io.Serializable;

public class LogicalStartTransaction extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    public LogicalStartTransaction() {
        super(OperationType.START_TRANSACTION);
    }
}
