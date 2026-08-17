package com.github.ares.connector.hive3.sink;

import com.github.ares.api.sink.AresSink;
import com.github.ares.connector.hive.sink.AbstractHiveSink;
import com.google.auto.service.AutoService;

@AutoService(AresSink.class)
public class Hive3Sink extends AbstractHiveSink {
    @Override
    public String getPluginName() {
        return "Hive3";
    }
}
