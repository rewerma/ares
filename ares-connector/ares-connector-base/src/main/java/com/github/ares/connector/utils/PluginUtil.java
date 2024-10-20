
package com.github.ares.connector.utils;


import com.github.ares.api.common.CommonOptions;
import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SourceTableInfo;
import com.github.ares.api.source.TableSource;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.CatalogTableUtil;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.api.table.factory.TableSourceFactoryContext;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.com.google.common.collect.Lists;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.configuration.utils.ConfigValidator;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connector.discovery.AresFactoryDiscovery;
import com.github.ares.connector.discovery.AresSinkPluginDiscovery;
import com.github.ares.connector.discovery.AresSourcePluginDiscovery;
import com.github.ares.connector.discovery.PluginIdentifier;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The util used for Spark/Flink to create to AresSource etc.
 */
public class PluginUtil {

    public static SourceTableInfo createSource(
            AresFactoryDiscovery factoryDiscovery,
            PluginIdentifier pluginIdentifier,
            Map<String, Object> pluginConfig) {
        // get current thread classloader
        ClassLoader classLoader =
                Thread.currentThread()
                        .getContextClassLoader(); // try to find factory of this plugin

        final ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(pluginConfig);
        // try to find table source factory
        final Optional<Factory> sourceFactory =
                factoryDiscovery.createOptionalPluginInstance(pluginIdentifier);
        final boolean fallback = isFallback(sourceFactory);
        AresSource source;
        if (fallback) {
            source = fallbackCreate(new AresSourcePluginDiscovery(), pluginIdentifier, readonlyConfig.toConfig());
        } else {
            // create source with source factory
            TableSourceFactoryContext context =
                    new TableSourceFactoryContext(readonlyConfig, classLoader);
            ConfigValidator.of(context.getOptions()).validate(sourceFactory.get().optionRule());
            TableSource tableSource =
                    ((TableSourceFactory) sourceFactory.get()).createSource(context);
            source = tableSource.createSource();
        }

        List<CatalogTable> catalogTables;
        try {
            catalogTables = source.getProducedCatalogTables();
        } catch (UnsupportedOperationException e) {
            // TODO remove it when all connector use `getProducedCatalogTables`
            AresDataType<?> aresDataType = source.getProducedType();
            final String tableId =
                    readonlyConfig.getOptional(CommonOptions.RESULT_TABLE_NAME).orElse("default-identifier");
            catalogTables =
                    CatalogTableUtil.convertDataTypeToCatalogTables(aresDataType, tableId);
        }

        if (catalogTables.size() != 1) {
            throw new AresException(
                    String.format("Unsupported table number: %d on flink", catalogTables.size()));
        }
        
        return new SourceTableInfo(source, catalogTables);
    }

    private static AresSource fallbackCreate(
            AresSourcePluginDiscovery sourcePluginDiscovery,
            PluginIdentifier pluginIdentifier,
            Config pluginConfig) {
        AresSource source = sourcePluginDiscovery.createPluginInstance(pluginIdentifier);
        source.prepare(pluginConfig);
        return source;
    }
    
    private static boolean isFallback(Optional<Factory> factory) {
        if (!factory.isPresent()) {
            return true;
        }
        try {
            ((TableSourceFactory) factory.get()).createSource(null);
        } catch (Exception e) {
            if (e instanceof UnsupportedOperationException
                    && "The Factory has not been implemented and the deprecated Plugin will be used."
                    .equals(e.getMessage())) {
                return true;
            }
        }
        return false;
    }

    public static Optional<? extends Factory> createSinkFactory(
            AresFactoryDiscovery factoryDiscovery,
            AresSinkPluginDiscovery sinkPluginDiscovery,
            String pluginName,
            List<URL> pluginJars) {
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of("sink", pluginName);
        pluginJars.addAll(
                sinkPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
        try {
            return factoryDiscovery.createOptionalPluginInstance(pluginIdentifier);
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

}
