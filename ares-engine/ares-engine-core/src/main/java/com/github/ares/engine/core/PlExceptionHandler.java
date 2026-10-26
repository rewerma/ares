package com.github.ares.engine.core;

import static com.github.ares.engine.utils.EngineUtil.handleQuoteIdentifier;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.parser.plan.LogicalExceptionHandler;
import java.util.function.Supplier;

/**
 * Shared EXCEPTION path for anonymous blocks, procedures, and functions: roll back the current
 * transaction first, then run the handler. Swallowed exceptions therefore never leave uncommitted
 * DML; RAISE rethrows after that.
 */
final class PlExceptionHandler {
    private PlExceptionHandler() {}

    static void handle(
            ExecutorManager executorManager,
            ExceptionMessageHandler messageHandler,
            LogicalExceptionHandler exHandler,
            Exception e,
            PlParams plParams,
            BodyCallback body) {
        executorManager.getTransactionManager().rollbackQuietly();
        String message = messageHandler.getMessage(e);
        message = handleQuoteIdentifier(message);
        plParams.put("ex.message", message, PlType.of(InternalFieldType.VARCHAR));
        body.invoke(exHandler.getExHandlerBody(), plParams);
        if (Boolean.TRUE.equals(exHandler.getWithRaise())) {
            throw new AresException(e);
        }
    }

    static <T> T run(
            ExecutorManager executorManager,
            ExceptionMessageHandler messageHandler,
            LogicalExceptionHandler exHandler,
            PlParams plParams,
            BodyCallback body,
            Supplier<T> action,
            Supplier<T> recovered) {
        if (exHandler == null) {
            return action.get();
        }
        try {
            return action.get();
        } catch (Exception e) {
            handle(executorManager, messageHandler, exHandler, e, plParams, body);
            return recovered == null ? null : recovered.get();
        }
    }
}
