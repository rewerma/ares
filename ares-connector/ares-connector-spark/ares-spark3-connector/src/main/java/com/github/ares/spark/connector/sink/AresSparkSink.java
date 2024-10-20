package com.github.ares.spark.connector.sink;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/** Ares sink class of Spark 3+, can be used as sink */
public class AresSparkSink implements DataSourceRegister, TableProvider {

    private static final String SINK_NAME = "AresSink";

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return null;
    }

    @Override
    public Table getTable(
            StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new AresSinkTable(properties);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    @Override
    public String shortName() {
        return SINK_NAME;
    }
}
