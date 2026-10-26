package com.github.ares.parser.config;

import com.github.ares.com.google.inject.Guice;
import com.github.ares.com.google.inject.Injector;
import com.github.ares.com.google.inject.Stage;

public final class ParserInjectorFactory {
    private ParserInjectorFactory() {}

    public static Injector create() {
        return Guice.createInjector(Stage.PRODUCTION, new ParserServiceModule());
    }
}
