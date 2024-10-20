package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalCreateTableAsSQL;

import java.io.Serializable;

public abstract class CreateTableAsSqlExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    public abstract void execute(LogicalCreateTableAsSQL createTableAsSql, PlParams plParams);
}
