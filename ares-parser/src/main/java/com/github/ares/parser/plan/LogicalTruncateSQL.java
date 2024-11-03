package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class LogicalTruncateSQL extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private LogicalCreateSinkTable sinkTable;
    private String originSQL;
    private String truncateSQL;
    private Boolean withEx;

    public LogicalTruncateSQL() {
        super(OperationType.TRUNCATE_SQL);
    }
}
