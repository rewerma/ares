package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class LogicalIfElse extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private LogicalExpression condition;
    private List<LogicalOperation> ifBody;

    private Boolean isIf;
    private Boolean isElseIf;
    private Boolean isElse;

    public LogicalIfElse() {
        super(OperationType.IF_ELSE);
    }
}
