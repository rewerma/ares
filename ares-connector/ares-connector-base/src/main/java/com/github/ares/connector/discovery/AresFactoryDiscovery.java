package com.github.ares.connector.discovery;

import com.github.ares.api.table.factory.Factory;
import org.apache.commons.lang3.StringUtils;

import java.net.URL;
import java.util.ServiceLoader;
import java.util.function.BiConsumer;

public class AresFactoryDiscovery extends AbstractPluginDiscovery<Factory> {

    private final Class<? extends Factory> factoryClass;

    public AresFactoryDiscovery(Class<? extends Factory> factoryClass) {
        super();
        this.factoryClass = factoryClass;
    }

    public AresFactoryDiscovery(
            Class<? extends Factory> factoryClass,
            BiConsumer<ClassLoader, URL> addURLToClassLoader) {
        super(addURLToClassLoader);
        this.factoryClass = factoryClass;
    }

    @Override
    protected Class<Factory> getPluginBaseClass() {
        return Factory.class;
    }

    @Override
    protected Factory loadPluginInstance(
            PluginIdentifier pluginIdentifier, ClassLoader classLoader) {
        ServiceLoader<Factory> serviceLoader =
                ServiceLoader.load(getPluginBaseClass(), classLoader);
        for (Factory factory : serviceLoader) {
            if (factoryClass.isInstance(factory)) {
                String factoryIdentifier = factory.factoryIdentifier();
                String pluginName = pluginIdentifier.getPluginName();
                if (StringUtils.equalsIgnoreCase(factoryIdentifier, pluginName)) {
                    return factory;
                }
            }
        }
        return null;
    }
}
