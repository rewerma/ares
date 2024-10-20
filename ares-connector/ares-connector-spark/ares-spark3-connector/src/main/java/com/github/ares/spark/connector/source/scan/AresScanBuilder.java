package com.github.ares.spark.connector.source.scan;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.table.type.AresRow;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** The builder for {@link AresScan} used to build {@link AresScan} */
public class AresScanBuilder implements ScanBuilder {
    private final AresSource<AresRow, ?, ?> source;

    private final int parallelism;

    private final CaseInsensitiveStringMap caseInsensitiveStringMap;

    public AresScanBuilder(
            AresSource<AresRow, ?, ?> source,
            int parallelism,
            CaseInsensitiveStringMap caseInsensitiveStringMap) {
        this.source = source;
        this.parallelism = parallelism;
        this.caseInsensitiveStringMap = caseInsensitiveStringMap;
    }

    /** Returns the {@link AresScan} */
    @Override
    public Scan build() {
        return new AresScan(source, parallelism, caseInsensitiveStringMap);
    }
}
