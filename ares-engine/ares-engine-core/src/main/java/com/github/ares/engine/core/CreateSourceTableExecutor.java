package com.github.ares.engine.core;

import com.github.ares.api.source.SourceTableInfo;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalOperation;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class CreateSourceTableExecutor extends AbstractBaseExecutor
        implements OperationHandler {
    private static final long serialVersionUID = -1L;

    protected Map<String, LogicalCreateSourceTable> createSourceTables = new ConcurrentHashMap<>();

    @Override
    public OperationType handledType() {
        return OperationType.CREATE_SOURCE_TABLE;
    }

    @Override
    public Scope scope() {
        return Scope.PROJECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalCreateSourceTable) operation);
        return lastData;
    }

    public void execute(LogicalCreateSourceTable sourceTable) {
        traceLogger.info(
                "Create source table: {}, connector type: {}",
                sourceTable.getTableName(),
                sourceTable.getConnector());
        createSourceTables.put(sourceTable.getTableName(), sourceTable);
        SourceTableInfo sourceTableInfo =
                executorManager.getSourceTables().get(sourceTable.getTableName());
        if (sourceTableInfo == null) {
            throw new AresException(
                    String.format("Source table undefined %s", sourceTable.getTableName()));
        }
        loadSource(sourceTable.getTableName(), sourceTableInfo);
    }

    public abstract void loadSource(String tableName, SourceTableInfo sourceTableInfo);
}
