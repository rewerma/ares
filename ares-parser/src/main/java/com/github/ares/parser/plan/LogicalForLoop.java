package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import java.io.Serializable;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LogicalForLoop extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private String indexName;
    private LogicalExpression lowerExpr;
    private LogicalExpression upperExpr;
    private List<LogicalOperation> forBody;

    public LogicalForLoop() {
        super(OperationType.FOR_LOOP);
    }

    public String conditionString() {
        return String.format(
                "%s IN %s ... %s", indexName, lowerExpr.getExpr(), upperExpr.getExpr());
    }
}
