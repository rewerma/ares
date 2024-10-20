package com.github.ares.engine.core;

import com.github.ares.api.source.SourceTableInfo;
import com.github.ares.common.exceptions.AresException;

public class ReloadFunctionExecutor extends AbstractBaseExecutor {

    public void reloadSourceTable(String sourceTable) {
        traceLogger.info("Reload source table: {}", sourceTable);
        SourceTableInfo sourceTableInfo = executorManager.getSourceTables().get(sourceTable);
        if (sourceTableInfo == null) {
            throw new AresException(String.format("Source table undefined %s", sourceTable));
        }
        this.executorManager.getCreateSourceTableExecutor().loadSource(sourceTable, sourceTableInfo);
    }
}
