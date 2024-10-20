package com.github.ares.engine.core;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.utils.ElapsedTimeWrapper;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalMergeIntoSQL;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public abstract class MergeIntoSqlExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    public void execute(LogicalMergeIntoSQL mergeIntoSQL, PlParams plParams) {
        Map<String, Optional<? extends Factory>> sinkPlugins = executorManager.getSinkPlugins();
        if (!sinkPlugins.containsKey(mergeIntoSQL.getSinkTable().getTableName())) {
            throw new AresException(String.format("Sink table undefined %s", mergeIntoSQL.getSinkTable().getTableName()));
        }
        ElapsedTimeWrapper.execute(mergeIntoSQL.getOriginSQL(), () -> {
            Optional<? extends Factory> sinkFactory = sinkPlugins.get(mergeIntoSQL.getSinkTable().getTableName());
            LogicalCreateSinkTable sinkTable = mergeIntoSQL.getSinkTable();
            execute(sinkTable.getOptions(), sinkFactory, mergeIntoSQL, plParams);
        });
    }

    public abstract void execute(Map<String, Object> sinkConfig, Optional<? extends Factory> sinkFactory,
                                 LogicalMergeIntoSQL mergeIntoSQL, PlParams plParams);

}
