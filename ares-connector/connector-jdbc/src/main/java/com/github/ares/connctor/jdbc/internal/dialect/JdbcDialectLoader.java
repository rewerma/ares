package com.github.ares.connctor.jdbc.internal.dialect;

import com.github.ares.connctor.jdbc.exception.JdbcConnectorException;
import com.github.ares.connctor.jdbc.internal.dialect.base.CommonDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** Utility for working with {@link JdbcDialect}. */
public final class JdbcDialectLoader {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialectLoader.class);

    private JdbcDialectLoader() {}

    public static JdbcDialect load(String url, String compatibleMode) {
        return load(url, compatibleMode, "");
    }

    /**
     * Loads the unique JDBC Dialect that can handle the given database url.
     *
     * @param url A database URL.
     * @param compatibleMode The compatible mode.
     * @throws IllegalStateException if the loader cannot find exactly one dialect that can
     *     unambiguously process the given database URL.
     * @return The loaded dialect.
     */
    public static JdbcDialect load(String url, String compatibleMode, String fieldIde) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        List<JdbcDialectFactory> foundFactories = discoverFactories(cl);

        if (foundFactories.isEmpty()) {
             throw new JdbcConnectorException(
                     String.format(
                             "Could not find any jdbc dialect factories that implement '%s' in the classpath.",
                             JdbcDialectFactory.class.getName()));
        }

        final List<JdbcDialectFactory> matchingFactories =
                foundFactories.stream().filter(f -> f.acceptsURL(url)).collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            return new CommonDialect();
        }
        if (matchingFactories.size() > 1) {
            throw new JdbcConnectorException(
                    String.format(
                            "Multiple jdbc dialect factories can handle url '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            url,
                            JdbcDialectFactory.class.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return matchingFactories.get(0).create(compatibleMode, fieldIde);
    }

    private static List<JdbcDialectFactory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<JdbcDialectFactory> result = new LinkedList<>();
            ServiceLoader.load(JdbcDialectFactory.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for jdbc dialects factory.", e);
            throw new JdbcConnectorException(
                    "Could not load service provider for jdbc dialects factory.",
                    e);
        }
    }
}
