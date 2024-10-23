package com.github.ares.engine.core;

import com.github.ares.api.common.PluginType;
import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.api.table.factory.TableSinkFactoryContext;
import com.github.ares.com.google.inject.Singleton;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.discovery.AresSinkPluginDiscovery;
import com.github.ares.connector.discovery.PluginIdentifier;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

import static com.github.ares.api.common.CommonOptions.CONNECTOR;

@Singleton
public class AresSinkFactory implements Serializable {
    private static final long serialVersionUID = 1L;

    public AresSink<?, ?, ?, ?> createSink(Map<String, Object> sinkConfig, Optional<? extends Factory> sinkFactory,
                                           CatalogTable catalogTable, TableSinkFactoryContext context) {
        boolean fallBack = !sinkFactory.isPresent() || isFallback(sinkFactory.get());
        AresSink<?, ?, ?, ?> sink;
        if (fallBack) {
            sink =
                    fallbackCreateSink(
                            new AresSinkPluginDiscovery(),
                            PluginIdentifier.of(
                                    PluginType.SINK.getType(),
                                    (String) sinkConfig.get(CONNECTOR.key())),
                            ReadonlyConfig.fromMap(sinkConfig).toConfig());
            sink.setTypeInfo(catalogTable.getAresRowType());
        } else {
            sink = ((TableSinkFactory) sinkFactory.get()).createSink(context).createSink();
        }
        return sink;
    }

    public boolean isFallback(Factory factory) {
        try {
            ((TableSinkFactory) factory).createSink(null);
        } catch (Exception e) {
            if (e instanceof UnsupportedOperationException
                    && "The Factory has not been implemented and the deprecated Plugin will be used."
                    .equals(e.getMessage())) {
                return true;
            }
        }
        return false;
    }

    public AresSink fallbackCreateSink(
            AresSinkPluginDiscovery sinkPluginDiscovery,
            PluginIdentifier pluginIdentifier,
            Config pluginConfig) {
        AresSink source = sinkPluginDiscovery.createPluginInstance(pluginIdentifier);
        source.prepare(pluginConfig);
        return source;
    }
}
