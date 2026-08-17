package com.github.ares.connector.hive.config;

import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import lombok.experimental.UtilityClass;

@UtilityClass
public class HiveFactoryOptions {

    public static OptionRule sourceOptionRule() {
        return baseOptionRule()
                .optional(BaseSourceConfigOptions.READ_PARTITIONS)
                .optional(BaseSourceConfigOptions.READ_COLUMNS)
                .build();
    }

    public static OptionRule sinkOptionRule() {
        return baseOptionRule().optional(HiveConfig.ABORT_DROP_PARTITION_METADATA).build();
    }

    private static OptionRule.Builder baseOptionRule() {
        return OptionRule.builder()
                .required(HiveConfig.TABLE_NAME)
                .required(HiveConfig.METASTORE_URI)
                .optional(HiveConfig.HIVE_SITE_PATH)
                .optional(HiveConfig.CATALOG_NAME)
                .optional(BaseSourceConfigOptions.HDFS_SITE_PATH)
                .optional(BaseSourceConfigOptions.REMOTE_USER)
                .optional(BaseSourceConfigOptions.KERBEROS_PRINCIPAL)
                .optional(BaseSourceConfigOptions.KRB5_PATH)
                .optional(BaseSourceConfigOptions.KERBEROS_KEYTAB_PATH);
    }
}
