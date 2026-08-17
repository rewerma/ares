package com.github.ares.connector.discovery;

import com.github.ares.api.common.PluginIdentifierInterface;
import com.github.ares.common.configuration.Common;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.utils.IsolatedClassLoader;
import com.github.ares.common.utils.PluginClassLoader;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class AbstractPluginDiscovery<T> implements PluginDiscovery<T> {
    private static final Logger log = LoggerFactory.getLogger(AbstractPluginDiscovery.class);

    private static final int COLLECTION_SIZE = 16;

    private final Path pluginDir;
    protected final ConcurrentHashMap<PluginIdentifier, Optional<URL>> pluginJarPath =
            new ConcurrentHashMap<>(COLLECTION_SIZE);

    protected AbstractPluginDiscovery() {
        this(Common.connectorDir());
    }

    protected AbstractPluginDiscovery(Path pluginDir) {
        this.pluginDir = pluginDir;
        log.info("Load {} Plugin from {}", getPluginBaseClass().getSimpleName(), pluginDir);
    }

    @Override
    public List<URL> getPluginJarPaths(List<PluginIdentifier> pluginIdentifiers) {
        return pluginIdentifiers.stream()
                .map(this::getPluginJarPath)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public List<T> getAllPlugins(List<PluginIdentifier> pluginIdentifiers) {
        return pluginIdentifiers.stream()
                .map(this::createPluginInstance)
                .distinct()
                .collect(Collectors.toList());
    }


    @Override
    public T createPluginInstance(PluginIdentifier pluginIdentifier) {
        return createPluginInstance(pluginIdentifier, Collections.emptyList());
    }

    @Override
    public Optional<T> createOptionalPluginInstance(PluginIdentifier pluginIdentifier) {
        return createOptionalPluginInstance(pluginIdentifier, Collections.emptyList());
    }

    @Override
    public Optional<T> createOptionalPluginInstance(
            PluginIdentifier pluginIdentifier, Collection<URL> pluginJars) {
        ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
        Optional<URL> pluginJarPathOp = getPluginJarPath(pluginIdentifier);
        // Prefer isolated loading from connectors dir even when the same jar is on classpath.
        if (pluginJarPathOp.isPresent()) {
            IsolatedClassLoader pluginClassLoader =
                    PluginClassLoader.createForDiscovery(
                            pluginJarPathOp.get(), pluginJars, parentClassLoader);
            T pluginInstance = loadPluginInstance(pluginIdentifier, pluginClassLoader);
            if (pluginInstance != null) {
                log.info(
                        "Load plugin: {} from path: {} use classloader: {}",
                        pluginIdentifier,
                        pluginJarPathOp.get(),
                        pluginClassLoader.getClass().getName());
                return Optional.of(pluginInstance);
            }
        }
        T pluginInstance = loadPluginInstance(pluginIdentifier, parentClassLoader);
        if (pluginInstance != null) {
            log.info("Load plugin: {} from classpath", pluginIdentifier);
            return Optional.of(pluginInstance);
        }
        return Optional.empty();
    }

    @Override
    public T createPluginInstance(PluginIdentifier pluginIdentifier, Collection<URL> pluginJars) {
        Optional<T> instance = createOptionalPluginInstance(pluginIdentifier, pluginJars);
        if (instance.isPresent()) {
            return instance.get();
        }
        throw new AresException("Plugin " + pluginIdentifier + " not found.");
    }

    /**
     * Get the plugin instance.
     *
     * @param pluginIdentifier plugin identifier.
     * @return plugin instance.
     */
    protected Optional<URL> getPluginJarPath(PluginIdentifier pluginIdentifier) {
        return pluginJarPath.computeIfAbsent(pluginIdentifier, this::findPluginJarPath);
    }

    @SuppressWarnings("unchecked")
    protected T loadPluginInstance(PluginIdentifier pluginIdentifier, ClassLoader classLoader) {
        ServiceLoader<T> serviceLoader = ServiceLoader.load(getPluginBaseClass(), classLoader);
        for (T t : serviceLoader) {
            if (t instanceof PluginIdentifierInterface) {
                // new api
                PluginIdentifierInterface pluginIdentifierInstance = (PluginIdentifierInterface) t;
                if (StringUtils.equalsIgnoreCase(
                        pluginIdentifierInstance.getPluginName(),
                        pluginIdentifier.getPluginName())) {
                    return (T) pluginIdentifierInstance;
                }
            } else {
                throw new UnsupportedOperationException(
                        "Plugin instance: " + t + " is not supported.");
            }
        }
        return null;
    }

    /**
     * Get spark plugin interface.
     *
     * @return plugin base class.
     */
    protected abstract Class<T> getPluginBaseClass();

    /**
     * Find the plugin jar path;
     *
     * @param pluginIdentifier plugin identifier.
     * @return plugin jar path.
     */
    private Optional<URL> findPluginJarPath(PluginIdentifier pluginIdentifier) {
        final String pluginName = pluginIdentifier.getPluginName();
        File[] targetPluginFiles =
                pluginDir
                        .toFile()
                        .listFiles(
                                pathname -> pathname.getName().endsWith(".jar")
                                        && StringUtils.startsWithIgnoreCase(
                                        pathname.getName(), "connector-" +
                                                StringUtils.join(StringUtils.splitByCharacterTypeCamelCase(pluginName), '-').toLowerCase()));
        if (ArrayUtils.isEmpty(targetPluginFiles)) {
            return Optional.empty();
        }
        if (targetPluginFiles.length > 1) {
            throw new IllegalArgumentException(
                    "Found multiple plugin jar: "
                            + Arrays.stream(targetPluginFiles)
                            .map(File::getPath)
                            .collect(Collectors.joining(","))
                            + " for pluginIdentifier: "
                            + pluginIdentifier);
        }
        try {
            URL pluginJarPathUrl = targetPluginFiles[0].toURI().toURL();
            log.info("Discovery plugin jar for: {} at: {}", pluginIdentifier, pluginJarPathUrl);
            return Optional.of(pluginJarPathUrl);
        } catch (MalformedURLException e) {
            log.warn(
                    "Cannot get plugin URL: {} for pluginIdentifier: {}" + targetPluginFiles[0],
                    pluginIdentifier,
                    e);
            return Optional.empty();
        }
    }
}
