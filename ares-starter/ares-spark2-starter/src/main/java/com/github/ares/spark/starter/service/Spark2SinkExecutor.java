package com.github.ares.spark.starter.service;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.common.utils.SerializationUtils;
import com.github.ares.engine.spark.core.SparkSinkExecutor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static com.github.ares.common.utils.Constants.SINK_SERIALIZATION;
import static com.github.ares.spark.connector.sink.SparkSinkInjector.SINK_CATALOG_TABLE;

public class Spark2SinkExecutor implements SparkSinkExecutor {
    private static final String SPARK_SINK_CLASS_NAME =
            "com.github.ares.spark.connector.sink.SparkSink";

    public void sink(Dataset<Row> dataset, AresSink<?, ?, ?, ?> aresSink, CatalogTable catalogTable) {
        dataset.write().format(SPARK_SINK_CLASS_NAME)
                .option(SINK_SERIALIZATION, SerializationUtils.objectToString(aresSink))
                .option(SINK_CATALOG_TABLE, SerializationUtils.objectToString(catalogTable))
                // .option("checkpointLocation", "/tmp")
                .save();
    }
}
