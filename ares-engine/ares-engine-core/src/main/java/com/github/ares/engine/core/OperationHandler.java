package com.github.ares.engine.core;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalOperation;
import java.io.Serializable;

/**
 * Self-describing statement handler. Implementing this on an executor is enough for {@link
 * ExecutorManager} to register it into the dispatcher.
 */
public interface OperationHandler extends Serializable {

    enum Scope {
        PROJECT,
        BODY,
        DIRECT
    }

    OperationType handledType();

    Scope scope();

    Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body);
}
