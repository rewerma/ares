package com.github.ares.engine.core;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.utils.ElapsedTimeWrapper;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalDeleteSelectSQL;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public abstract class DeleteSelectSqlExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    public void execute(LogicalDeleteSelectSQL deleteSelectSQL, PlParams plParams) {
        Map<String, Optional<? extends Factory>> sinkPlugins = executorManager.getSinkPlugins();
        if (!sinkPlugins.containsKey(deleteSelectSQL.getSinkTable().getTableName())) {
            throw new AresException(String.format("Sink table undefined %s", deleteSelectSQL.getSinkTable().getTableName()));
        }
        ElapsedTimeWrapper.execute(deleteSelectSQL.getOriginSQL(), () -> {
            Optional<? extends Factory> sinkFactory = sinkPlugins.get(deleteSelectSQL.getSinkTable().getTableName());
            LogicalCreateSinkTable sinkTable = deleteSelectSQL.getSinkTable();
            execute(sinkTable.getOptions(), sinkFactory, deleteSelectSQL, plParams);
        });
    }

    public abstract void execute(Map<String, Object> sinkConfig, Optional<? extends Factory> sinkFactory,
                                 LogicalDeleteSelectSQL dsSql, PlParams plParams);

}
