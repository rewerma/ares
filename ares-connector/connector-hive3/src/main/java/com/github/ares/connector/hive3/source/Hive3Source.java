package com.github.ares.connector.hive3.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.connector.hive.source.AbstractHiveSource;
import com.google.auto.service.AutoService;

@AutoService(AresSource.class)
public class Hive3Source extends AbstractHiveSource {
    @Override
    public String getPluginName() {
        return "Hive3";
    }
}
