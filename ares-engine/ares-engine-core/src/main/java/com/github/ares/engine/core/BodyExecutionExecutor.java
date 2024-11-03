package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalAssignment;
import com.github.ares.parser.plan.LogicalForCursorLoop;
import com.github.ares.parser.plan.LogicalForLoop;
import com.github.ares.parser.plan.LogicalIfElse;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalReturnValue;
import com.github.ares.parser.plan.LogicalSelectIntoSQL;
import com.github.ares.parser.plan.LogicalWhileLoop;

import java.io.Serializable;
import java.util.List;

import static com.github.ares.parser.enums.OperationType.CONTINUE_LOOP;
import static com.github.ares.parser.enums.OperationType.EXIT_LOOP;
import static com.github.ares.parser.enums.OperationType.RETURN_VALUE;

public class BodyExecutionExecutor extends AbstractBaseExecutor implements IBodyExecutor, Serializable {
    private static final long serialVersionUID = -1L;

    public Object execute(List<LogicalOperation> operations, PlParams plParams) {
        int i = 0;
        Object lastData = null;
        while (i < operations.size()) {
            LogicalOperation operation = operations.get(i);
            if (operation == null) {
                i++;
                continue;
            }
            switch (operation.getOperationType()) {
                case ASSIGNMENT:
                    executorManager.getAssignmentExecutor().execute((LogicalAssignment) operation, plParams);
                    break;
                case IF_ELSE:
                    int idx = executorManager.getIfElseExecutor().ifElse((LogicalIfElse) operation,
                            plParams, i, operations, this::execute);
                    if (idx == IfElseExecutor.BREAK_LOOP_FLAG) { // with loop exit
                        return EXIT_LOOP;
                    } else if (idx == IfElseExecutor.CONTINUE_LOOP_FLAG) { // with function return
                        return CONTINUE_LOOP;
                    }  else if (idx == IfElseExecutor.RETURN_FUNCTION_FLAG) { // with function return
                        return RETURN_VALUE;
                    } else {
                        i = idx;
                    }
                    break;
                case WHILE_LOOP:
                    lastData = executorManager.getWhileLoopExecutor().execute((LogicalWhileLoop) operation, plParams, this::execute);
                    break;
                case FOR_LOOP:
                    lastData = executorManager.getForLoopExecutor().execute((LogicalForLoop) operation, plParams, this::execute);
                    break;
                case EXIT_LOOP:
                    traceLogger.info("Loop: EXIT");
                    return EXIT_LOOP;
                case CONTINUE_LOOP:
                    traceLogger.info("Loop: CONTINUE");
                    return CONTINUE_LOOP;
                case FOR_CURSOR_LOOP:
                    lastData = executorManager.getForCursorLoopExecutor().execute((LogicalForCursorLoop) operation, plParams, this::execute);
                    break;
                case RETURN_VALUE:
                    executorManager.getReturnFunctionExecutor().execute((LogicalReturnValue) operation, plParams);
                    return RETURN_VALUE;
                case SELECT_INTO_SQL:
                    executorManager.selectIntoSqlExecutor.execute((LogicalSelectIntoSQL) operation, plParams);
                    break;
                default:
                    lastData = executorManager.directExecutionExecutor.execute(operation, plParams, lastData);
                    break;
            }
            i++;
        }
        return lastData;
    }
}
