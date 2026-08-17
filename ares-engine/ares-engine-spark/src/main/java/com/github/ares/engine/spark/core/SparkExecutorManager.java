package com.github.ares.engine.spark.core;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.com.google.inject.Inject;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.parser.config.PlProperties;
import java.io.Serializable;
import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Getter
public class SparkExecutorManager extends ExecutorManager implements Serializable {
    private static final long serialVersionUID = -1L;

    @Inject private SparkSessionManager sparkSessionManager;
    @Inject private SparkSinkExecutor sparkSinkExecutor;
    @Inject private SparkCommonExecutor sparkCommonExecutor;

    public void init(PlProperties plProperties, SparkSession sparkSession) {
        sparkSessionManager.init(sparkSession);
        super.init(plProperties);
        getTransactionManager().init(new SparkJdbcTransactionalSinkHandler(this));
    }

    public boolean tryTransactionalSink(
            AresSink<?, ?, ?, ?> aresSink,
            Dataset<Row> dataset,
            CatalogTable catalogTable,
            String sinkTableName) {
        if (!getTransactionManager().isActive()) {
            return false;
        }
        getTransactionManager().write(aresSink, dataset, catalogTable, sinkTableName);
        return true;
    }
}
