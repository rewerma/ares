package com.github.ares.common.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.net.URL;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Unified entry point for plugin classloader operations: jar discovery, isolated classloader
 * creation, and plugin deserialization across driver and executor.
 */
public final class PluginClassLoader {

    private PluginClassLoader() {}

    public static URL[] getPluginJarUrls(Class<?> pluginClass) {
        Set<URL> jarUrls = new LinkedHashSet<>();
        if (pluginClass.getProtectionDomain().getCodeSource() != null) {
            URL codeSource = pluginClass.getProtectionDomain().getCodeSource().getLocation();
            if (codeSource != null) {
                jarUrls.add(codeSource);
            }
        }
        collectConnectorJarUrls(pluginClass.getClassLoader(), jarUrls);
        return jarUrls.toArray(new URL[0]);
    }

    public static boolean requiresIsolatedClassLoader(Class<?> pluginClass) {
        URL[] jarUrls = getPluginJarUrls(pluginClass);
        return jarUrls.length > 0 && isJarUrl(jarUrls[0]);
    }

    public static IsolatedClassLoader createIsolated(Class<?> pluginClass, ClassLoader parent) {
        return createIsolated(getPluginJarUrls(pluginClass), parent);
    }

    public static IsolatedClassLoader createIsolated(URL[] jarUrls, ClassLoader parent) {
        if (jarUrls == null || jarUrls.length == 0) {
            return null;
        }
        return new IsolatedClassLoader(jarUrls, parent);
    }

    public static IsolatedClassLoader createForDiscovery(
            URL pluginJar, Collection<URL> additionalJars, ClassLoader parent) {
        Set<URL> jarUrls = new LinkedHashSet<>();
        jarUrls.add(pluginJar);
        if (additionalJars != null) {
            jarUrls.addAll(additionalJars);
        }
        return new IsolatedClassLoader(jarUrls.toArray(new URL[0]), parent);
    }

    public static <T extends Serializable> T deserializePlugin(
            String base64Serialization, ClassLoader parentClassLoader) {
        if (StringUtils.isEmpty(base64Serialization)) {
            return null;
        }
        T plugin = SerializationUtils.stringToObject(base64Serialization);
        if (plugin == null) {
            return null;
        }
        if (!requiresIsolatedClassLoader(plugin.getClass())) {
            return plugin;
        }
        IsolatedClassLoader classLoader = createIsolated(plugin.getClass(), parentClassLoader);
        byte[] pluginBytes = Base64.getDecoder().decode(base64Serialization);
        return SerializationUtils.deserialize(pluginBytes, classLoader);
    }

    public static <T extends Serializable> T reloadInPluginClassLoader(
            T plugin, ClassLoader parentClassLoader) {
        if (plugin == null) {
            return null;
        }
        if (!requiresIsolatedClassLoader(plugin.getClass())) {
            return plugin;
        }
        String serialized = SerializationUtils.objectToString(plugin);
        IsolatedClassLoader classLoader = createIsolated(plugin.getClass(), parentClassLoader);
        byte[] pluginBytes = Base64.getDecoder().decode(serialized);
        return SerializationUtils.deserialize(pluginBytes, classLoader);
    }

    public static <T> T callWithContextClassLoader(
            ClassLoader classLoader, Supplier<T> action) {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            return action.get();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private static void collectConnectorJarUrls(ClassLoader classLoader, Set<URL> jarUrls) {
        ClassLoader current = classLoader;
        while (current != null) {
            if (current instanceof IsolatedClassLoader) {
                for (URL url : ((IsolatedClassLoader) current).getURLs()) {
                    if (isConnectorJar(url)) {
                        jarUrls.add(url);
                    }
                }
            } else if (current instanceof java.net.URLClassLoader) {
                for (URL url : ((java.net.URLClassLoader) current).getURLs()) {
                    if (isConnectorJar(url)) {
                        jarUrls.add(url);
                    }
                }
            }
            current = current.getParent();
        }
    }

    private static boolean isConnectorJar(URL url) {
        String path = url.getFile();
        return path != null && path.endsWith(".jar") && path.contains("connector-");
    }

    private static boolean isJarUrl(URL url) {
        String path = url.getFile();
        return path != null && path.endsWith(".jar");
    }
}
