package com.github.ares.connector.hive3.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.hive.config.HiveFactoryOptions;
import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class Hive3SourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Hive3";
    }

    @Override
    public OptionRule optionRule() {
        return HiveFactoryOptions.sourceOptionRule();
    }

    @Override
    public Class<? extends AresSource> getSourceClass() {
        return Hive3Source.class;
    }
}
