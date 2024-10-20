package com.github.ares.parser.sqlparser;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

public class SQLParserFactoryLoader {
    private static final String DEFAULT_TYPE = "JSQLParser";

    private static volatile SQLParserFactory sqlParserFactory;

    public static SQLParserFactory getDefaultFactory() {
        if (sqlParserFactory == null) {
            synchronized (SQLParserFactoryLoader.class) {
                if (sqlParserFactory == null) {
                    sqlParserFactory = loadFactory(DEFAULT_TYPE);
                }
            }
        }
        return sqlParserFactory;
    }

    public static SQLParserFactory loadFactory(String type) {
        List<SQLParserFactory> factories = discoverFactories();
        for (SQLParserFactory factory : factories) {
            if (factory.getType().equals(type)) {
                return factory;
            }
        }
        throw new IllegalArgumentException("No SQL parser factory found for type: " + type);
    }

    public static List<SQLParserFactory> discoverFactories() {
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            final List<SQLParserFactory> result = new LinkedList<>();
            ServiceLoader.load(SQLParserFactory.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            throw new RuntimeException("Could not load service provider for sql parser factory.", e);
        }
    }
}
