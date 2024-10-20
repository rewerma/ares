package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.model.Argument;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class LogicalAssignment extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = 1L;

    private Argument param;
    private String expr;

    public LogicalAssignment() {
        super(OperationType.ASSIGNMENT);
    }
}
