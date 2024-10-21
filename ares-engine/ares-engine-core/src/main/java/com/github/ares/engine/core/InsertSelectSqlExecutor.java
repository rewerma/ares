package com.github.ares.engine.core;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.utils.ElapsedTimeWrapper;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalInsertSelectSQL;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public abstract class InsertSelectSqlExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    public void execute(LogicalInsertSelectSQL insertSelectSQL, PlParams plParams) {
        Map<String, Optional<? extends Factory>> sinkPlugins = executorManager.getSinkPlugins();
        if (!sinkPlugins.containsKey(insertSelectSQL.getSinkTable().getTableName())) {
            throw new AresException(String.format("Sink table undefined %s", insertSelectSQL.getSinkTable().getTableName()));
        }

        ElapsedTimeWrapper.execute(insertSelectSQL.getOriginSQL(), () -> {
            Optional<? extends Factory> sinkFactory = sinkPlugins.get(insertSelectSQL.getSinkTable().getTableName());
            LogicalCreateSinkTable sinkTable = insertSelectSQL.getSinkTable();
            execute(sinkTable.getOptions(), sinkFactory, insertSelectSQL, plParams);
        });
    }

    public abstract void execute(Map<String, Object> sinkConfig, Optional<? extends Factory> sinkFactory,
                                 LogicalInsertSelectSQL isSql, PlParams plParams);
}
