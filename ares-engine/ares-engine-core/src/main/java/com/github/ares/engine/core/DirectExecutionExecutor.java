package com.github.ares.engine.core;

import com.github.ares.common.exceptions.AresException;
import com.github.ares.parser.plan.LogicalCallFunction;
import com.github.ares.parser.plan.LogicalCreateTableAsSQL;
import com.github.ares.parser.plan.LogicalDeleteSelectSQL;
import com.github.ares.parser.plan.LogicalInsertSelectSQL;
import com.github.ares.parser.plan.LogicalMergeIntoSQL;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalSelectSQL;
import com.github.ares.parser.plan.LogicalTruncateSQL;
import com.github.ares.parser.plan.LogicalUpdateSelectSQL;

import java.io.Serializable;
import java.util.List;

public class DirectExecutionExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    public Object execute(List<LogicalOperation> operations) {
        PlParams plParams = new PlParams();
        Object lastData = null;
        for (LogicalOperation operation : operations) {
            if (operation == null) {
                continue;
            }
            lastData = execute(operation, plParams, lastData);
        }
        return lastData;
    }

    public Object execute(LogicalOperation operation, PlParams plParams, Object lastData) {
        switch (operation.getOperationType()) {
            case CALL_FUNCTION:
                executorManager.callFunctionExecutor.execute((LogicalCallFunction) operation, plParams);
                break;
            case CREATE_TABLE_AS_SQL:
                executorManager.getCreateTableAsSqlExecutor().execute((LogicalCreateTableAsSQL) operation, plParams);
                break;
            case SELECT_SQL:
                lastData = executorManager.getSelectSqlExecutor().execute((LogicalSelectSQL) operation, plParams, lastData);
                break;
            case INSERT_SELECT_SQL:
                executorManager.getInsertSelectSqlExecutor().execute((LogicalInsertSelectSQL) operation, plParams);
                break;
            case UPDATE_SELECT_SQL:
                executorManager.getUpdateSelectSqlExecutor().execute((LogicalUpdateSelectSQL) operation, plParams);
                break;
            case DELETE_SELECT_SQL:
                executorManager.getDeleteSelectSqlExecutor().execute((LogicalDeleteSelectSQL) operation, plParams);
                break;
            case MERGE_INTO_SQL:
                executorManager.getMergeIntoSqlExecutor().execute((LogicalMergeIntoSQL) operation, plParams);
                break;
            case TRUNCATE_SQL:
                executorManager.getTruncateSqlExecutor().execute((LogicalTruncateSQL) operation);
                break;
            case SET_CONFIG:
                break;
            default:
                throw new AresException(String.format("Unsupported script syntax block: %s",
                        operation.getOperationType().getName()));
        }
        return lastData;
    }
}
