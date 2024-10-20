package com.github.ares.engine.core;

import com.github.ares.api.source.SourceTableInfo;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.parser.plan.LogicalCreateSourceTable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class CreateSourceTableExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    protected Map<String,LogicalCreateSourceTable> createSourceTables = new HashMap<>();

    public void execute(LogicalCreateSourceTable sourceTable, Map<String, SourceTableInfo> sourceTables) {
        traceLogger.info("Create source table: {}, connector type: {}",
                sourceTable.getTableName(), sourceTable.getConnector());
        createSourceTables.put(sourceTable.getTableName(), sourceTable);
        SourceTableInfo sourceTableInfo = sourceTables.get(sourceTable.getTableName());
        if (sourceTableInfo == null) {
            throw new AresException(String.format("Source table undefined %s", sourceTable.getTableName()));
        }
        loadSource(sourceTable.getTableName(), sourceTableInfo);
    }

    public abstract void loadSource(String tableName, SourceTableInfo sourceTableInfo);
}
