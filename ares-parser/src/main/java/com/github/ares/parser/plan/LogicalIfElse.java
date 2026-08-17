package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LogicalIfElse extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private LogicalExpression condition;
    private List<LogicalOperation> ifBody;
    private List<LogicalIfElse> elseIfs = new ArrayList<>();
    private List<LogicalOperation> elseBody;

    public LogicalIfElse() {
        super(OperationType.IF_ELSE);
    }
}
