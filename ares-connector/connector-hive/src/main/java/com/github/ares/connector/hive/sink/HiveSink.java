package com.github.ares.connector.hive.sink;

import com.github.ares.api.sink.AresSink;
import com.google.auto.service.AutoService;

@AutoService(AresSink.class)
public class HiveSink extends AbstractHiveSink {
    @Override
    public String getPluginName() {
        return "Hive";
    }
}
