package com.github.ares.common.utils;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

public class IsolatedClassLoader extends URLClassLoader {
    private static final String[] DEFAULT_PARENT_FIRST_PATTERNS =
            new String[] {
                "java.",
                "javax.xml",
                "org.xml",
                "org.w3c",
                "scala.",
                "javax.annotation.",
                "org.slf4j",
                "org.apache.log4j",
                "org.apache.logging",
                "org.apache.commons.logging",
                "com.fasterxml.jackson",
                "org.apache.spark.",
                "com.github.ares.api.",
                "com.github.ares.common."
            };

    static {
        ClassLoader.registerAsParallelCapable();
    }

    public IsolatedClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> c = findLoadedClass(name);
            if (c == null) {
                if (shouldUseParentFirst(name)) {
                    return super.loadClass(name, resolve);
                }

                try {
                    c = findClass(name);
                } catch (ClassNotFoundException e) {
                    c = super.loadClass(name, resolve);
                }
            }
            if (resolve) {
                resolveClass(c);
            }
            return c;
        }
    }

    @Override
    public URL getResource(String name) {
        if (shouldUseParentFirstResource(name)) {
            return super.getResource(name);
        }
        URL url = findResource(name);
        if (url != null) {
            return url;
        }
        ClassLoader parent = getParent();
        return parent != null ? parent.getResource(name) : null;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        if (shouldUseParentFirstResource(name)) {
            return super.getResources(name);
        }
        List<URL> resources = new ArrayList<>();
        Enumeration<URL> childResources = findResources(name);
        while (childResources.hasMoreElements()) {
            resources.add(childResources.nextElement());
        }
        ClassLoader parent = getParent();
        if (parent != null) {
            Enumeration<URL> parentResources = parent.getResources(name);
            while (parentResources.hasMoreElements()) {
                resources.add(parentResources.nextElement());
            }
        }
        return Collections.enumeration(resources);
    }

    private static boolean shouldUseParentFirst(String name) {
        for (String alwaysParentFirstPattern : DEFAULT_PARENT_FIRST_PATTERNS) {
            if (name.startsWith(alwaysParentFirstPattern)) {
                return true;
            }
        }
        return false;
    }

    private static boolean shouldUseParentFirstResource(String name) {
        return name.startsWith("META-INF/services/org.slf4j")
                || name.startsWith("META-INF/services/org.apache.logging")
                || name.startsWith("META-INF/services/org.apache.spark");
    }
}
