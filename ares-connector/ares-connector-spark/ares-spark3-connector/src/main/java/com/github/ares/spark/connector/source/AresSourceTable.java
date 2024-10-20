package com.github.ares.spark.connector.source;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.api.source.AresSource;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.utils.Constants;
import com.github.ares.common.utils.SerializationUtils;
import com.github.ares.spark.connector.source.scan.AresScanBuilder;
import com.github.ares.spark.connector.utils.TypeConverterUtils;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;
import java.util.Set;

/** The basic unit of Ares DataSource generated, supporting read and write */
public class AresSourceTable implements Table, SupportsRead {
    private static final String SOURCE_TABLE_NAME = "AresSourceTable";

    private final Map<String, String> properties;

    private final AresSource<AresRow, ?, ?> source;

    public AresSourceTable(Map<String, String> properties) {
        this.properties = properties;
        String sourceSerialization = properties.getOrDefault(Constants.SOURCE_SERIALIZATION, "");
        if (StringUtils.isBlank(sourceSerialization)) {
            throw new IllegalArgumentException("source.serialization must be specified");
        }
        this.source = SerializationUtils.stringToObject(sourceSerialization);
    }

    /**
     * Returns a {@link ScanBuilder} which can be used to build a {@link Scan}
     *
     * @param caseInsensitiveStringMap The options for reading, which is an immutable
     *     case-insensitive string-to-string map.
     */
    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        int parallelism =
                Integer.parseInt(properties.getOrDefault(CommonOptions.PARALLELISM.key(), "1"));
        return new AresScanBuilder(source, parallelism, caseInsensitiveStringMap);
    }

    /** A name to identify this table */
    @Override
    public String name() {
        return SOURCE_TABLE_NAME;
    }

    /** Returns the schema of this table */
    @Override
    public StructType schema() {
        return (StructType) TypeConverterUtils.convert(source.getProducedType());
    }

    /** Returns the set of capabilities for this table */
    @Override
    public Set<TableCapability> capabilities() {
        return Sets.newHashSet(TableCapability.BATCH_READ, TableCapability.MICRO_BATCH_READ);
    }
}
