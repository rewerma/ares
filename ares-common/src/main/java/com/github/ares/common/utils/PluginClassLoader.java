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
        URL codeSource = getCodeSourceLocation(pluginClass);
        if (codeSource != null) {
            jarUrls.add(codeSource);
        }
        if (pluginClass.getClassLoader() instanceof IsolatedClassLoader) {
            String pluginJarName = extractPluginJarName(codeSource);
            for (URL url : ((IsolatedClassLoader) pluginClass.getClassLoader()).getURLs()) {
                if (isSamePluginJar(url, pluginJarName)) {
                    jarUrls.add(url);
                }
            }
        }
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

    public static void runWithContextClassLoader(ClassLoader classLoader, Runnable action) {
        callWithContextClassLoader(
                classLoader,
                () -> {
                    action.run();
                    return null;
                });
    }

    private static URL getCodeSourceLocation(Class<?> pluginClass) {
        if (pluginClass.getProtectionDomain() == null
                || pluginClass.getProtectionDomain().getCodeSource() == null) {
            return null;
        }
        URL codeSource = pluginClass.getProtectionDomain().getCodeSource().getLocation();
        if (codeSource == null || !isJarUrl(codeSource)) {
            return null;
        }
        return codeSource;
    }

    private static String extractPluginJarName(URL codeSource) {
        if (codeSource == null) {
            return null;
        }
        String path = codeSource.getFile();
        if (path == null) {
            return null;
        }
        int separator = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
        return separator >= 0 ? path.substring(separator + 1) : path;
    }

    private static boolean isSamePluginJar(URL url, String pluginJarName) {
        if (url == null || !isJarUrl(url) || StringUtils.isEmpty(pluginJarName)) {
            return false;
        }
        String path = url.getFile();
        return path != null && path.endsWith(pluginJarName);
    }

    private static boolean isJarUrl(URL url) {
        String path = url.getFile();
        return path != null && path.endsWith(".jar");
    }
}
