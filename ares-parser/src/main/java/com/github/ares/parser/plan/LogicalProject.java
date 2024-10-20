package com.github.ares.parser.plan;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class LogicalProject implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<LogicalOperation> logicalOperations;

    public List<LogicalCreateSourceTable> getSourceTables() {
        List<LogicalCreateSourceTable> sourceTables = new ArrayList<>();
        if (logicalOperations == null) {
            return sourceTables;
        }
        for (LogicalOperation logicalOperation : logicalOperations) {
            if (logicalOperation instanceof LogicalCreateSourceTable) {
                sourceTables.add((LogicalCreateSourceTable) logicalOperation);
            }
        }
        return sourceTables;
    }

    public List<LogicalCreateSinkTable> getSinkTables() {
        List<LogicalCreateSinkTable> sinkTables = new ArrayList<>();
        if (logicalOperations == null) {
            return sinkTables;
        }
        for (LogicalOperation logicalOperation : logicalOperations) {
            if (logicalOperation instanceof LogicalCreateSinkTable) {
                sinkTables.add((LogicalCreateSinkTable) logicalOperation);
            }
        }
        return sinkTables;
    }
}
