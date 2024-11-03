package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class LogicalWhileLoop extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private LogicalExpression condition;
    private List<LogicalOperation> whileBody;

    public LogicalWhileLoop() {
        super(OperationType.WHILE_LOOP);
    }
}
