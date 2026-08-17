package com.github.ares.connector.hive.source;

import com.github.ares.api.source.AresSource;
import com.google.auto.service.AutoService;

@AutoService(AresSource.class)
public class HiveSource extends AbstractHiveSource {
    @Override
    public String getPluginName() {
        return "Hive";
    }
}
