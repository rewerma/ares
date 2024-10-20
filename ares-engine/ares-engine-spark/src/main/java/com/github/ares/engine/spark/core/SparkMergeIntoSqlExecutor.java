package com.github.ares.engine.spark.core;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.com.google.inject.Inject;
import com.github.ares.com.google.inject.Singleton;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.InsertSelectSqlExecutor;
import com.github.ares.engine.core.MergeIntoSqlExecutor;
import com.github.ares.engine.core.PlParams;
import com.github.ares.engine.core.UpdateSelectSqlExecutor;
import com.github.ares.parser.plan.LogicalInsertSelectSQL;
import com.github.ares.parser.plan.LogicalMergeIntoSQL;
import com.github.ares.parser.plan.LogicalUpdateSelectSQL;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;


public class SparkMergeIntoSqlExecutor extends MergeIntoSqlExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    private SparkExecutorManager sparkExecutorManager;

    public void init(ExecutorManager executorManager) {
        this.sparkExecutorManager = (SparkExecutorManager) executorManager;
        super.init(executorManager);
    }

    @Override
    public void execute(Map<String, Object> sinkConfig, Optional<? extends Factory> sinkFactory,
                        LogicalMergeIntoSQL mergeIntoSql, PlParams plParams) {
        traceLogger.info("SQL: {}; Params: {}", mergeIntoSql.getOriginSQL(), plParams);
        if (StringUtils.isNotBlank(mergeIntoSql.getUpdateSourceSql())) {


            String updateSourceSql = replaceParams(mergeIntoSql.getUpdateSourceSql(), plParams);
            Map<String, Object> sinkConfig4Update = new HashMap<>(sinkConfig);
            LogicalUpdateSelectSQL updateSelectSQL = new LogicalUpdateSelectSQL();
            updateSelectSQL.setUpdateItems(mergeIntoSql.getUpdateColumns());
            updateSelectSQL.setWhereClause(mergeIntoSql.getAllWaitCriteria());
            updateSelectSQL.setSelectSQL(updateSourceSql);
            updateSelectSQL.setWithShow(mergeIntoSql.getWithShow());
            updateSelectSQL.setRepartitionNums(mergeIntoSql.getRepartitionNums());
            updateSelectSQL.setRepartitionColumns(mergeIntoSql.getRepartitionColumns());
            sparkExecutorManager.getUpdateSelectSqlExecutor().execute(sinkConfig4Update, sinkFactory, updateSelectSQL, plParams);
        }

        if (StringUtils.isNotBlank(mergeIntoSql.getInsertSourceSql())) {
            String insertSourceSql = replaceParams(mergeIntoSql.getInsertSourceSql(), plParams);
            Map<String, Object> sinkConfig4Insert = new HashMap<>(sinkConfig);
            LogicalInsertSelectSQL insertSelectSQL = new LogicalInsertSelectSQL();
            insertSelectSQL.setSelectSQL(insertSourceSql);
            insertSelectSQL.setTargetColumns(mergeIntoSql.getInsertColumns());
            insertSelectSQL.setWithShow(mergeIntoSql.getWithShow());
            insertSelectSQL.setRepartitionNums(mergeIntoSql.getRepartitionNums());
            insertSelectSQL.setRepartitionColumns(mergeIntoSql.getRepartitionColumns());
            sparkExecutorManager.getInsertSelectSqlExecutor().execute(sinkConfig4Insert, sinkFactory, insertSelectSQL, plParams);
        }
    }

}
