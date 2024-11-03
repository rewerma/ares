package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalSelectIntoSQL;

import java.io.Serializable;

public abstract class SelectIntoSqlExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    public void commonFunction() {
    }

    public abstract void execute(LogicalSelectIntoSQL selectIntoSQL, PlParams plParams);
}
