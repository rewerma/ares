package com.github.ares.connector.hive.sink;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.hive.config.HiveConfig;
import com.google.auto.service.AutoService;

import static com.github.ares.connector.hive.config.HiveConfig.ABORT_DROP_PARTITION_METADATA;

@AutoService(Factory.class)
public class HiveSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Hive";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HiveConfig.TABLE_NAME)
                .required(HiveConfig.METASTORE_URI)
                .optional(ABORT_DROP_PARTITION_METADATA)
                .build();
    }
}
