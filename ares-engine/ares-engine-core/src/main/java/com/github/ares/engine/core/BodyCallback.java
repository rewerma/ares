package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalOperation;

import java.io.Serializable;
import java.util.List;

public interface BodyCallback extends Serializable {
    Object invoke(List<LogicalOperation> operations, PlParams plParams);
}
