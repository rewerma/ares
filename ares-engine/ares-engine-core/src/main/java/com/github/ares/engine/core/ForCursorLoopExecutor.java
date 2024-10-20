package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalForCursorLoop;

import java.io.Serializable;

public abstract class ForCursorLoopExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    public abstract Object execute(LogicalForCursorLoop forCursorLoop, PlParams plParams, BodyCallback bodyCallback);
}
