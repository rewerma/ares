package com.github.ares.engine.core;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalExceptionHandler;
import com.github.ares.parser.plan.LogicalOperation;

public class TryCatchExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Inject private ExceptionMessageHandler exceptionMessageHandler;

    @Override
    public OperationType handledType() {
        return OperationType.EXCEPTION_HANDLER;
    }

    @Override
    public Scope scope() {
        return Scope.BODY;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        LogicalExceptionHandler handler = (LogicalExceptionHandler) operation;
        Object result =
                PlExceptionHandler.run(
                        executorManager,
                        exceptionMessageHandler,
                        handler,
                        plParams,
                        body,
                        () -> body.invoke(handler.getTryBody(), plParams),
                        () -> lastData);
        if (LoopControl.isSignal(result)) {
            return result;
        }
        return result != null ? result : lastData;
    }
}
