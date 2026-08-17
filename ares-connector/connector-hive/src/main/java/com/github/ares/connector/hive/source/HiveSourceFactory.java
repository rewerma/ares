package com.github.ares.connector.hive.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.hive.config.HiveFactoryOptions;
import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class HiveSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Hive";
    }

    @Override
    public OptionRule optionRule() {
        return HiveFactoryOptions.sourceOptionRule();
    }

    @Override
    public Class<? extends AresSource> getSourceClass() {
        return HiveSource.class;
    }
}
