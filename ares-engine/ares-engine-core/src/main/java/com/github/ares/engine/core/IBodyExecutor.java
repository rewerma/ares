package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalOperation;

import java.io.Serializable;
import java.util.List;

public interface IBodyExecutor extends Serializable {
    Object execute(List<LogicalOperation> operations, PlParams plParams);
}
