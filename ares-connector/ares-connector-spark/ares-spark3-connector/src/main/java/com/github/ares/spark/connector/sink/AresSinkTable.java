package com.github.ares.spark.connector.sink;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.utils.Constants;
import com.github.ares.common.utils.SerializationUtils;
import com.github.ares.spark.connector.sink.write.AresWriteBuilder;
import com.github.ares.spark.connector.utils.TypeConverterUtils;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.Set;

public class AresSinkTable implements Table, SupportsWrite {

    private static final String SINK_TABLE_NAME = "AresSinkTable";

    private final AresSink<AresRow, ?, ?, ?> aresSink;

    private final CatalogTable catalogTable;

    public AresSinkTable(Map<String, String> properties) {
        String sinkSerialization = properties.getOrDefault(Constants.SINK_SERIALIZATION, "");
        if (StringUtils.isBlank(sinkSerialization)) {
            throw new IllegalArgumentException(Constants.SINK_SERIALIZATION + " must be specified");
        }
        this.aresSink = SerializationUtils.stringToObject(sinkSerialization);
        String sinkCatalogTableSerialization =
                properties.getOrDefault(SparkSinkInjector.SINK_CATALOG_TABLE, "");
        if (StringUtils.isBlank(sinkCatalogTableSerialization)) {
            throw new IllegalArgumentException(
                    SparkSinkInjector.SINK_CATALOG_TABLE + " must be specified");
        }
        this.catalogTable = SerializationUtils.stringToObject(sinkCatalogTableSerialization);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new AresWriteBuilder<>(aresSink, catalogTable);
    }

    @Override
    public String name() {
        return SINK_TABLE_NAME;
    }

    @Override
    public StructType schema() {
        return (StructType) TypeConverterUtils.convert(catalogTable.getAresRowType());
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Sets.newHashSet(TableCapability.BATCH_WRITE, TableCapability.STREAMING_WRITE);
    }
}
