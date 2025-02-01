package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class LogicalSelectSQL extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private String originSQL;

    private String sql;

    public LogicalSelectSQL() {
        super(OperationType.SELECT_SQL);
    }
}
