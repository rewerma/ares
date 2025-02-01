package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class LogicalAnonymousBody extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private LogicalDeclareParams declareParams;

    private List<LogicalOperation> anonymousBody = new ArrayList<>();

    private LogicalExceptionHandler exHandler;

    public LogicalAnonymousBody() {
        super(OperationType.ANONYMOUS_BODY);
    }
}
