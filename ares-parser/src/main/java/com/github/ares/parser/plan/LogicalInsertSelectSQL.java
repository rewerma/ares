package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.model.BaseSqlOption;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;

@Getter
@Setter
public class LogicalInsertSelectSQL extends BaseSqlOption implements Serializable {
    private static final long serialVersionUID = 1L;

    private LogicalCreateSinkTable sinkTable;
    private ArrayList<String> targetColumns;
    private String selectSQL;

    private Boolean withEx;

    public LogicalInsertSelectSQL() {
        super(OperationType.INSERT_SELECT_SQL);
    }
}
