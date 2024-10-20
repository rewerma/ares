package com.github.ares.connector.discovery;

import com.github.ares.api.common.PluginIdentifierInterface;
import com.github.ares.common.configuration.Common;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.utils.ReflectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public abstract class AbstractPluginDiscovery<T> implements PluginDiscovery<T> {
    private static final Logger log = LoggerFactory.getLogger(AbstractPluginDiscovery.class);

    private static final int COLLECTION_SIZE = 16;

    /**
     * Add jar url to classloader. The different engine should have different logic to add url into
     * their own classloader
     */
    private static final BiConsumer<ClassLoader, URL> DEFAULT_URL_TO_CLASSLOADER =
            (classLoader, url) -> {
                if (classLoader instanceof URLClassLoader) {
                    ReflectionUtils.invoke(classLoader, "addURL", url);
                } else {
                    throw new UnsupportedOperationException("can't support custom load jar");
                }
            };

    private final Path pluginDir;
    private final BiConsumer<ClassLoader, URL> addURLToClassLoaderConsumer;
    protected final ConcurrentHashMap<PluginIdentifier, Optional<URL>> pluginJarPath =
            new ConcurrentHashMap<>(COLLECTION_SIZE);


    protected AbstractPluginDiscovery() {
        this(Common.connectorDir());
    }

    protected AbstractPluginDiscovery(BiConsumer<ClassLoader, URL> addURLToClassloader) {
        this(Common.connectorDir(), addURLToClassloader);
    }

    protected AbstractPluginDiscovery(Path pluginDir) {
        this(pluginDir, DEFAULT_URL_TO_CLASSLOADER);
    }

    protected AbstractPluginDiscovery(
            Path pluginDir,
            BiConsumer<ClassLoader, URL> addURLToClassLoaderConsumer) {
        this.pluginDir = pluginDir;
        this.addURLToClassLoaderConsumer = addURLToClassLoaderConsumer;
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
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        T pluginInstance = loadPluginInstance(pluginIdentifier, classLoader);
        if (pluginInstance != null) {
            log.info("Load plugin: {} from classpath", pluginIdentifier);
            return Optional.of(pluginInstance);
        }
        Optional<URL> pluginJarPathOp = getPluginJarPath(pluginIdentifier);
        // if the plugin jar not exist in classpath, will load from plugin dir.
        if (pluginJarPathOp.isPresent()) {
            try {
                // use current thread classloader to avoid different classloader load same class
                // error.
                this.addURLToClassLoaderConsumer.accept(classLoader, pluginJarPathOp.get());
                for (URL jar : pluginJars) {
                    addURLToClassLoaderConsumer.accept(classLoader, jar);
                }
            } catch (Exception e) {
                log.warn("can't load jar use current thread classloader, use URLClassLoader instead now."
                        + " message: {}", e.getMessage());
                URL[] urls = new URL[pluginJars.size() + 1];
                int i = 0;
                for (URL pluginJar : pluginJars) {
                    urls[i++] = pluginJar;
                }
                urls[i] = pluginJarPathOp.get();
                classLoader =
                        new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
            }
            pluginInstance = loadPluginInstance(pluginIdentifier, classLoader);
            if (pluginInstance != null) {
                log.info(
                        "Load plugin: {} from path: {} use classloader: {}",
                        pluginIdentifier,
                        pluginJarPathOp.get(),
                        classLoader.getClass().getName());
                return Optional.of(pluginInstance);
            }
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
