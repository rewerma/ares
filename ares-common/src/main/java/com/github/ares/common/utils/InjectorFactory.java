package com.github.ares.common.utils;

import com.github.ares.com.google.inject.Injector;

public class InjectorFactory {
    private static volatile Injector injector;

    public static Injector init(Injector injectorInstance) {
        if (injector == null) {
            synchronized (InjectorFactory.class) {
                if (injector == null) {
                    injector = injectorInstance;
                }
            }
        }
        return injectorInstance;
    }

    public static Injector getInjector() {
        return injector;
    }
}
