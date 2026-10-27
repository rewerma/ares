package com.github.ares.parser.visitor;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.model.SourceSinkTable;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalOperation;
import java.util.Map;

public class PlCreateSourceTableVisitor {
    private Map<String, LogicalCreateSourceTable> sourceTables;

    public void init(Map<String, LogicalCreateSourceTable> sourceTables) {
        this.sourceTables = sourceTables;
    }

    public LogicalOperation visitCreateSourceTable(
            String tableName, Map<String, Object> withOptions) {
        String normalized = SourceSinkTable.normalizeAlias(tableName);
        if (sourceTables.containsKey(normalized)) {
            throw new ParseException(String.format("Source table name exists: %s", tableName));
        }
        LogicalCreateSourceTable sourceTable = new LogicalCreateSourceTable();
        sourceTable.setTableName(normalized);
        sourceTable.getOptions().putAll(withOptions);
        sourceTable.setConnector((String) sourceTable.getOptions().get("connector"));
        sourceTables.put(normalized, sourceTable);
        return sourceTable;
    }
}
