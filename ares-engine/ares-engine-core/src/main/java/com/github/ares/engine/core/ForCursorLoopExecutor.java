package com.github.ares.engine.core;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalForCursorLoop;
import com.github.ares.parser.plan.LogicalOperation;

public abstract class ForCursorLoopExecutor extends AbstractBaseExecutor
        implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.FOR_CURSOR_LOOP;
    }

    @Override
    public Scope scope() {
        return Scope.BODY;
    }

    @Override
    public Object handle(
            LogicalOperation operation,
            PlParams plParams,
            Object lastData,
            BodyCallback bodyCallback) {
        return execute((LogicalForCursorLoop) operation, plParams, bodyCallback);
    }

    public abstract Object execute(
            LogicalForCursorLoop forCursorLoop, PlParams plParams, BodyCallback bodyCallback);
}
