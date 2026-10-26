package com.github.ares.connector.discovery;

import com.github.ares.api.sink.AresSink;
import java.nio.file.Path;

public class AresSinkPluginDiscovery extends AbstractPluginDiscovery<AresSink> {

    public AresSinkPluginDiscovery() {
        super();
    }

    public AresSinkPluginDiscovery(Path pluginDir) {
        super(pluginDir);
    }

    @Override
    protected Class<AresSink> getPluginBaseClass() {
        return AresSink.class;
    }
}
