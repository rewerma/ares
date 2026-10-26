package com.github.ares.engine.core;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalAnonymousBody;
import com.github.ares.parser.plan.LogicalDeclareParams;
import com.github.ares.parser.plan.LogicalExceptionHandler;
import com.github.ares.parser.plan.LogicalOperation;

public class AnonymousBodyExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Inject private ExceptionMessageHandler exceptionMessageHandler;

    @Override
    public OperationType handledType() {
        return OperationType.ANONYMOUS_BODY;
    }

    @Override
    public Scope scope() {
        return Scope.PROJECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        Object lastResult = execute((LogicalAnonymousBody) operation);
        return lastResult != null ? lastResult : lastData;
    }

    public Object execute(LogicalAnonymousBody anonymousBody) {
        traceLogger.info("Anonymous body: BEGIN");
        PlParams plParams = new PlParams();
        LogicalDeclareParams declareOperation = anonymousBody.getDeclareParams();
        if (declareOperation != null) {
            executorManager.getDeclareParamsExecutor().execute(declareOperation, plParams);
        }
        Object result = null;
        LogicalExceptionHandler exHandler = anonymousBody.getExHandler();
        BodyCallback body = executorManager.getBodyExecutionExecutor()::execute;
        try {
            result =
                    PlExceptionHandler.run(
                            executorManager,
                            exceptionMessageHandler,
                            exHandler,
                            plParams,
                            body,
                            () ->
                                    executorManager
                                            .getBodyExecutionExecutor()
                                            .execute(anonymousBody.getAnonymousBody(), plParams),
                            null);
        } catch (Exception e) {
            executorManager.getTransactionManager().rollbackQuietly();
            throw e;
        } finally {
            executorManager.getTransactionManager().close();
        }
        traceLogger.info("Anonymous body: END");
        return result;
    }
}
