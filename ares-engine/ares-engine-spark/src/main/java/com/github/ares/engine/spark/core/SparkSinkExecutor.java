package com.github.ares.engine.spark.core;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public interface SparkSinkExecutor extends Serializable {
    void sink(Dataset<Row> dataset, AresSink<?, ?, ?, ?> aresSink, CatalogTable catalogTable);
}
