package com.github.ares.spark.starter.service;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.common.utils.Constants;
import com.github.ares.common.utils.SerializationUtils;
import com.github.ares.engine.spark.core.SparkSinkExecutor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class Spark3SinkExecutor implements SparkSinkExecutor {

    private static final String SINK_NAME = AresSink.class.getSimpleName();
    public static final String SINK_CATALOG_TABLE = "sink.catalog.table";

    public void sink(Dataset<Row> dataset, AresSink<?, ?, ?, ?> aresSink, CatalogTable catalogTable) {
        dataset.write().format(SINK_NAME)
                .option(Constants.SINK_SERIALIZATION, SerializationUtils.objectToString(aresSink))
                .option(SINK_CATALOG_TABLE, SerializationUtils.objectToString(catalogTable))
                .mode(SaveMode.Append)
                .save();
    }
}
