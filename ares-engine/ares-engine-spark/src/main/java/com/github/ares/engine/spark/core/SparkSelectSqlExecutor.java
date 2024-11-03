package com.github.ares.engine.spark.core;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.SelectSqlExecutor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSelectSqlExecutor extends SelectSqlExecutor {
    private static final long serialVersionUID = -1L;

    private SparkExecutorManager sparkExecutorManager;

    public void init(ExecutorManager executorManager) {
        this.sparkExecutorManager = (SparkExecutorManager) executorManager;
        super.init(executorManager);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object executeSelectSql(String sql, Object lastData) {
        SparkSession sparkSession = sparkExecutorManager.getSparkSessionManager().getSparkSession();

        Dataset<Row> resultDf = sparkSession.sql(sql);
        Dataset<Row> lastDf = (Dataset<Row>) lastData;
        if (lastDf != null) {
            lastDf.unpersist();
        }
        lastDf = resultDf.limit(100);
        lastDf.cache();
        lastDf.show();
        return lastDf;
    }

}
