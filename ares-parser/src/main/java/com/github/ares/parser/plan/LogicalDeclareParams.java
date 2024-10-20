package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.model.Argument;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class LogicalDeclareParams extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<Argument> declareParams;

    public LogicalDeclareParams() {
        super(OperationType.DECLARE_PARAMS);
    }
}
