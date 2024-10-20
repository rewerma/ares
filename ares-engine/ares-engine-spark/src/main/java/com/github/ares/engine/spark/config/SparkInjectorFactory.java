package com.github.ares.engine.spark.config;

import com.github.ares.com.google.inject.AbstractModule;
import com.github.ares.com.google.inject.Guice;
import com.github.ares.com.google.inject.Injector;
import com.github.ares.common.utils.InjectorFactory;
import com.github.ares.engine.config.BaseServiceModule;
import com.github.ares.parser.config.ParserServiceModule;

public final class SparkInjectorFactory {
    private static volatile Injector injector;

    public static synchronized void init(AbstractModule... modules) {
        if (injector == null) {
            synchronized (SparkInjectorFactory.class) {
                if (injector == null) {
                    AbstractModule[] abstractModules = new AbstractModule[modules.length + 3];
                    abstractModules[0] = new ParserServiceModule();
                    abstractModules[1] = new BaseServiceModule();
                    abstractModules[2] = new SparkServiceModule();
                    System.arraycopy(modules, 0, abstractModules, 3, modules.length);
                    injector = InjectorFactory.init(Guice.createInjector(abstractModules));
                }
            }
        }
    }

    public static Injector getInjector() {
        return injector;
    }
}
