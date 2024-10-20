package com.github.ares.connector.discovery;

import com.github.ares.api.sink.AresSink;

import java.net.URL;
import java.nio.file.Path;
import java.util.function.BiConsumer;

public class AresSinkPluginDiscovery extends AbstractPluginDiscovery<AresSink> {

    public AresSinkPluginDiscovery() {
        super();
    }

    public AresSinkPluginDiscovery(Path pluginDir) {
        super(pluginDir);
    }

    public AresSinkPluginDiscovery(BiConsumer<ClassLoader, URL> addURLToClassLoader) {
        super(addURLToClassLoader);
    }

    @Override
    protected Class<AresSink> getPluginBaseClass() {
        return AresSink.class;
    }
}
