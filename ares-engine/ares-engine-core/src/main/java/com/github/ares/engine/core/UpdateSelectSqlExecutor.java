package com.github.ares.engine.core;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.utils.ElapsedTimeWrapper;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalUpdateSelectSQL;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public abstract class UpdateSelectSqlExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    public void execute(LogicalUpdateSelectSQL updateSelectSQL, PlParams plParams) {
        Map<String, Optional<? extends Factory>> sinkPlugins = executorManager.getSinkPlugins();
        if (!sinkPlugins.containsKey(updateSelectSQL.getSinkTable().getTableName())) {
            throw new AresException(String.format("Sink table undefined %s", updateSelectSQL.getSinkTable().getTableName()));
        }
        ElapsedTimeWrapper.execute(updateSelectSQL.getOriginSQL(), () -> {
            Optional<? extends Factory> sinkFactory = sinkPlugins.get(updateSelectSQL.getSinkTable().getTableName());
            LogicalCreateSinkTable sinkTable = updateSelectSQL.getSinkTable();
            execute(sinkTable.getOptions(), sinkFactory, updateSelectSQL, plParams);
        });
    }

    public abstract void execute(Map<String, Object> sinkConfig, Optional<? extends Factory> sinkFactory,
                                 LogicalUpdateSelectSQL usSql, PlParams plParams);

}
