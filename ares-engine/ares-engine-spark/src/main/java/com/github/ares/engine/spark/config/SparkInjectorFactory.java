package com.github.ares.engine.spark.config;

import com.github.ares.com.google.inject.AbstractModule;
import com.github.ares.com.google.inject.Guice;
import com.github.ares.com.google.inject.Injector;
import com.github.ares.com.google.inject.Module;
import com.github.ares.com.google.inject.Stage;
import com.github.ares.com.google.inject.util.Modules;
import com.github.ares.common.utils.InjectorFactory;
import com.github.ares.engine.config.BaseServiceModule;
import com.github.ares.parser.config.ParserServiceModule;

public final class SparkInjectorFactory {
    private static volatile Injector injector;

    /**
     * Extra modules override {@link SparkServiceModule} so the Spark 2/3 starter can bind {@code
     * SparkSinkExecutor} to a concrete DataSource writer. Guice 7 rejects a second {@code
     * bind(SparkSinkExecutor)} as a sibling of the untargeted interface binding.
     */
    public static synchronized void init(AbstractModule... modules) {
        if (injector == null) {
            synchronized (SparkInjectorFactory.class) {
                if (injector == null) {
                    Module sparkEngine = new SparkServiceModule();
                    if (modules != null && modules.length > 0) {
                        sparkEngine = Modules.override(sparkEngine).with(modules);
                    }
                    injector =
                            InjectorFactory.init(
                                    Guice.createInjector(
                                            Stage.PRODUCTION,
                                            new ParserServiceModule(),
                                            new BaseServiceModule(),
                                            sparkEngine));
                }
            }
        }
    }

    public static Injector getInjector() {
        return injector;
    }
}
