package com.github.ares.connector.hive.sink;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.hive.config.HiveFactoryOptions;
import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class HiveSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Hive";
    }

    @Override
    public OptionRule optionRule() {
        return HiveFactoryOptions.sinkOptionRule();
    }
}
