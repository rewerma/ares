package com.github.ares.connector.hive.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import com.github.ares.connector.hive.config.HiveConfig;
import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class HiveSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Hive";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HiveConfig.TABLE_NAME)
                .required(HiveConfig.METASTORE_URI)
                .optional(BaseSourceConfigOptions.READ_PARTITIONS)
                .optional(BaseSourceConfigOptions.READ_COLUMNS)
                .build();
    }

    @Override
    public Class<? extends AresSource> getSourceClass() {
        return HiveSource.class;
    }
}
