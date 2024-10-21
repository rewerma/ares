package com.github.ares.connector.hive.config;

import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.Options;
import com.github.ares.connector.hive.utils.HiveMetaStoreProxy;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.Table;

public class HiveConfig {
    public static final Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hive table name");
    public static final Option<String> METASTORE_URI =
            Options.key("metastore_uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hive metastore uri");

    public static final Option<Boolean> ABORT_DROP_PARTITION_METADATA =
            Options.key("abort_drop_partition_metadata")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Flag to decide whether to drop partition metadata from Hive Metastore during an abort operation. Note: this only affects the metadata in the metastore, the data in the partition will always be deleted(data generated during the synchronization process).");

    public static final Option<String> HIVE_SITE_PATH =
            Options.key("hive_site_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The path of hive-site.xml");

    public static final String TEXT_INPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.mapred.TextInputFormat";
    public static final String TEXT_OUTPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    public static final String PARQUET_INPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    public static final String PARQUET_OUTPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    public static final String ORC_INPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    public static final String ORC_OUTPUT_FORMAT_CLASSNAME =
            "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";

    public static Pair<String[], Table> getTableInfo(Config config) {
        String table = config.getString(TABLE_NAME.key());
        String[] splits = table.split("\\.");
        if (splits.length != 2) {
            throw new RuntimeException("Please config " + TABLE_NAME + " as db.table format");
        }
        HiveMetaStoreProxy hiveMetaStoreProxy = HiveMetaStoreProxy.getInstance(config);
        Table tableInformation = hiveMetaStoreProxy.getTable(splits[0], splits[1]);
        hiveMetaStoreProxy.close();
        return Pair.of(splits, tableInformation);
    }
}
