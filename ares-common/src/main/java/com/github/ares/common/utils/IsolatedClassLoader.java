package com.github.ares.common.utils;

import java.net.URL;
import java.net.URLClassLoader;

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
                    "com.fasterxml.jackson"
            };

    public IsolatedClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> c = findLoadedClass(name);
            if (c == null) {
                // check whether the class should go parent-first
                for (String alwaysParentFirstPattern : DEFAULT_PARENT_FIRST_PATTERNS) {
                    if (name.startsWith(alwaysParentFirstPattern)) {
                        return super.loadClass(name, resolve);
                    }
                }

                try {
                    c = findClass(name);
                } catch (ClassNotFoundException e) {
                    c = super.loadClass(name, resolve);
                }
            }
            return c;
        }
    }
}