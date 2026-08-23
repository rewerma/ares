package com.github.ares.engine.core;

import static com.github.ares.parser.enums.OperationType.CONTINUE_LOOP;
import static com.github.ares.parser.enums.OperationType.EXIT_LOOP;
import static com.github.ares.parser.enums.OperationType.SET_CONFIG;

import com.github.ares.common.exceptions.AresException;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalOperation;
import java.io.Serializable;
import java.util.EnumMap;
import java.util.Map;

/** OperationType → handler maps for project / body / direct execution. */
public class OperationDispatcher implements Serializable {
    private static final long serialVersionUID = -1L;

    private final Map<OperationType, OperationHandler> projectHandlers =
            new EnumMap<>(OperationType.class);
    private final Map<OperationType, OperationHandler> bodyHandlers =
            new EnumMap<>(OperationType.class);
    private final Map<OperationType, OperationHandler> directHandlers =
            new EnumMap<>(OperationType.class);

    public void register(OperationHandler handler) {
        Map<OperationType, OperationHandler> target = mapFor(handler.scope());
        OperationType type = handler.handledType();
        OperationHandler previous = target.put(type, handler);
        if (previous != null) {
            throw new AresException(
                    "Duplicate operation handler for "
                            + type.getName()
                            + ": "
                            + previous.getClass().getName()
                            + " and "
                            + handler.getClass().getName());
        }
    }

    public void registerBuiltins(TraceLogger traceLogger) {
        bodyHandlers.put(
                EXIT_LOOP,
                new BuiltinHandler(
                        EXIT_LOOP,
                        OperationHandler.Scope.BODY,
                        (op, params, lastData, body) -> {
                            traceLogger.info("Loop: EXIT");
                            return EXIT_LOOP;
                        }));
        bodyHandlers.put(
                CONTINUE_LOOP,
                new BuiltinHandler(
                        CONTINUE_LOOP,
                        OperationHandler.Scope.BODY,
                        (op, params, lastData, body) -> {
                            traceLogger.info("Loop: CONTINUE");
                            return CONTINUE_LOOP;
                        }));
        directHandlers.put(
                SET_CONFIG,
                new BuiltinHandler(
                        SET_CONFIG,
                        OperationHandler.Scope.DIRECT,
                        (op, params, lastData, body) -> lastData));
    }

    public OperationHandler bodyHandler(OperationType type) {
        return bodyHandlers.get(type);
    }

    public Object dispatchProject(LogicalOperation operation, Object lastData) {
        OperationHandler handler = projectHandlers.get(operation.getOperationType());
        if (handler != null) {
            return handler.handle(operation, new PlParams(), lastData, null);
        }
        return dispatchDirect(operation, new PlParams(), lastData);
    }

    public Object dispatchDirect(LogicalOperation operation, PlParams plParams, Object lastData) {
        OperationHandler handler = directHandlers.get(operation.getOperationType());
        if (handler == null) {
            throw new AresException(
                    String.format(
                            "Unsupported script syntax block: %s",
                            operation.getOperationType().getName()));
        }
        return handler.handle(operation, plParams, lastData, null);
    }

    private Map<OperationType, OperationHandler> mapFor(OperationHandler.Scope scope) {
        switch (scope) {
            case PROJECT:
                return projectHandlers;
            case BODY:
                return bodyHandlers;
            case DIRECT:
                return directHandlers;
            default:
                throw new AresException("Unknown handler scope: " + scope);
        }
    }

    @FunctionalInterface
    private interface HandleFn extends Serializable {
        Object handle(
                LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body);
    }

    private static final class BuiltinHandler implements OperationHandler {
        private static final long serialVersionUID = -1L;
        private final OperationType type;
        private final Scope scope;
        private final HandleFn delegate;

        private BuiltinHandler(OperationType type, Scope scope, HandleFn delegate) {
            this.type = type;
            this.scope = scope;
            this.delegate = delegate;
        }

        @Override
        public OperationType handledType() {
            return type;
        }

        @Override
        public Scope scope() {
            return scope;
        }

        @Override
        public Object handle(
                LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
            return delegate.handle(operation, plParams, lastData, body);
        }
    }
}
