package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;

import com.github.ares.parser.model.TableWith;

import java.io.Serializable;

public class LogicalCreateSinkTable extends TableWith implements Serializable {
    private static final long serialVersionUID = 1L;

    public LogicalCreateSinkTable(String connector) {
        super(OperationType.CREATE_SINK_TABLE);
        this.connector = connector;
    }
}
