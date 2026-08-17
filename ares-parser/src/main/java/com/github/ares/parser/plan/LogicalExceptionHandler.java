package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LogicalExceptionHandler extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private List<LogicalOperation> exHandlerBody = new ArrayList<>();

    private Boolean withRaise;

    public LogicalExceptionHandler() {
        super(OperationType.EXCEPTION_HANDLER);
    }
}
