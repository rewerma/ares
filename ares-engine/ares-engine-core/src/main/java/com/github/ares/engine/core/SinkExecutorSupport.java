package com.github.ares.engine.core;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.utils.ElapsedTimeWrapper;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

/** Shared sink lookup + timed execution for INSERT / UPDATE / DELETE / MERGE. */
final class SinkExecutorSupport {
    private SinkExecutorSupport() {}

    static Optional<? extends Factory> requireSinkFactory(
            ExecutorManager executorManager, LogicalCreateSinkTable sinkTable) {
        String tableName = sinkTable.getTableName();
        Map<String, Optional<? extends Factory>> sinkPlugins = executorManager.getSinkPlugins();
        if (!sinkPlugins.containsKey(tableName)) {
            throw new AresException(String.format("Sink table undefined %s", tableName));
        }
        return sinkPlugins.get(tableName);
    }

    static void execute(
            ExecutorManager executorManager,
            LogicalCreateSinkTable sinkTable,
            String originSql,
            BiConsumer<Map<String, Object>, Optional<? extends Factory>> action) {
        Optional<? extends Factory> sinkFactory = requireSinkFactory(executorManager, sinkTable);
        ElapsedTimeWrapper.execute(
                originSql, () -> action.accept(sinkTable.getOptions(), sinkFactory));
    }
}
