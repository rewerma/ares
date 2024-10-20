package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class LogicalForCursorLoop extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = 1L;

    private String cursorName;
    private String selectSQL;
    private List<LogicalOperation> forBody;

    public LogicalForCursorLoop() {
        super(OperationType.FOR_CURSOR_LOOP);
    }
}
