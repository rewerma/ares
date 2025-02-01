package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.model.Argument;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

@Getter
@Setter
public class LogicalCallFunction extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private String funcName;

    private List<LogicalExpression> args = new ArrayList<>();

    private List<Argument> outArgs;

    public LogicalCallFunction() {
        super(OperationType.CALL_FUNCTION);
    }

    public String getArgsString() {
        StringJoiner joiner = new StringJoiner(", ");
        for (LogicalExpression arg : args) {
            joiner.add(arg.getExpr());
        }
        return joiner.toString();
    }
}
