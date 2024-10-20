package com.github.ares.engine.core;

import com.github.ares.api.source.SourceTableInfo;
import com.github.ares.com.google.inject.Inject;
import com.github.ares.com.google.inject.Singleton;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.parser.plan.LogicalAnonymousBody;
import com.github.ares.parser.plan.LogicalAssignment;
import com.github.ares.parser.plan.LogicalCallFunction;
import com.github.ares.parser.plan.LogicalCreateFunction;
import com.github.ares.parser.plan.LogicalCreateProcedure;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalCreateTableAsSQL;
import com.github.ares.parser.plan.LogicalDeclareParams;
import com.github.ares.parser.plan.LogicalDeleteSelectSQL;
import com.github.ares.parser.plan.LogicalForCursorLoop;
import com.github.ares.parser.plan.LogicalForLoop;
import com.github.ares.parser.plan.LogicalIfElse;
import com.github.ares.parser.plan.LogicalInsertSelectSQL;
import com.github.ares.parser.plan.LogicalMergeIntoSQL;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalSelectIntoSQL;
import com.github.ares.parser.plan.LogicalSelectSQL;
import com.github.ares.parser.plan.LogicalTruncateSQL;
import com.github.ares.parser.plan.LogicalUpdateSelectSQL;
import com.github.ares.parser.plan.LogicalWhileLoop;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.github.ares.parser.enums.OperationType.EXIT_LOOP;

public class ProjectExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, SourceTableInfo> sourceTables;

    private Object lastData;

    public void init(ExecutorManager executorManager) {
        super.init(executorManager);
        this.sourceTables = executorManager.getSourceTables();
    }

    public Object execute(List<LogicalOperation> operations) {
        for (LogicalOperation operation : operations) {
            if (operation == null) {
                continue;
            }
            switch (operation.getOperationType()) {
                case CREATE_SOURCE_TABLE:
                    executorManager.getCreateSourceTableExecutor().execute((LogicalCreateSourceTable) operation, sourceTables);
                    break;
                case CREATE_SINK_TABLE:
                    executorManager.getCreateSinkTableExecutor().execute((LogicalCreateSinkTable) operation);
                    break;
                case ANONYMOUS_BODY:
                    lastData = executorManager.getAnonymousBodyExecutor().execute((LogicalAnonymousBody) operation);
                    break;
                case CREATE_PROCEDURE:
                    executorManager.getCreateProcedureExecutor().execute((LogicalCreateProcedure) operation);
                    break;
                case CREATE_FUNCTION:
                    executorManager.createFunctionExecutor.execute((LogicalCreateFunction) operation);
                    break;
                default:
                    lastData = executorManager.directExecutionExecutor.execute(operation, new PlParams(), lastData);
                    break;
            }
        }
        return lastData;
    }
}
