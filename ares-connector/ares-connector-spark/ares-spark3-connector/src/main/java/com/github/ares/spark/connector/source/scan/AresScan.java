package com.github.ares.spark.connector.source.scan;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.spark.connector.source.partition.batch.AresBatch;
import com.github.ares.spark.connector.source.partition.micro.AresMicroBatch;
import com.github.ares.spark.connector.utils.TypeConverterUtils;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class AresScan implements Scan {

    private final AresSource<AresRow, ?, ?> source;

    private final int parallelism;

    private final CaseInsensitiveStringMap caseInsensitiveStringMap;

    public AresScan(
            AresSource<AresRow, ?, ?> source,
            int parallelism,
            CaseInsensitiveStringMap caseInsensitiveStringMap) {
        this.source = source;
        this.parallelism = parallelism;
        this.caseInsensitiveStringMap = caseInsensitiveStringMap;
    }

    @Override
    public StructType readSchema() {
        return (StructType) TypeConverterUtils.convert(source.getProducedType());
    }

    @Override
    public Batch toBatch() {
        Map<String, String> envOptions = caseInsensitiveStringMap.asCaseSensitiveMap();
        return new AresBatch(source, parallelism, envOptions);
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return new AresMicroBatch(
                source, parallelism, checkpointLocation, caseInsensitiveStringMap);
    }
}
