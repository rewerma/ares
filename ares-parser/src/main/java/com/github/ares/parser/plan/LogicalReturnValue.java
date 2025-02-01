package com.github.ares.parser.plan;

import com.github.ares.common.engine.PlType;
import com.github.ares.parser.enums.OperationType;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class LogicalReturnValue extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private PlType plType;
    private String expr;

    public LogicalReturnValue(PlType plType) {
        super(OperationType.RETURN_VALUE);
        this.plType = plType;
    }
}
