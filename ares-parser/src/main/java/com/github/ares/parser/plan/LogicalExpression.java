package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LogicalExpression extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private String expr;

    public LogicalExpression() {
        super(OperationType.EXPRESSION);
    }
}
