package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.connector.jdbc.exception.JdbcConnectorException;
import com.github.ares.connector.jdbc.internal.dialect.base.CommonDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Utility for working with {@link JdbcDialect}.
 */
public final class JdbcDialectLoader {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialectLoader.class);

    private JdbcDialectLoader() {
    }

    public static JdbcDialect load(String dbType, String url, String compatibleMode) {
        return load(dbType, url, compatibleMode, "");
    }

    /**
     * Loads the unique JDBC Dialect that can handle the given database url.
     *
     * @param dbType         dbType.
     * @param compatibleMode The compatible mode.
     * @return The loaded dialect.
     * @throws IllegalStateException if the loader cannot find exactly one dialect that can
     *                               unambiguously process the given database URL.
     */
    public static JdbcDialect load(String dbType, String url, String compatibleMode, String fieldIde) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        List<JdbcDialectFactory> foundFactories = discoverFactories(cl);

        final List<JdbcDialectFactory> matchingFactories =
                foundFactories.stream().filter(f -> {
                    if (f.dialectIdentifier().equalsIgnoreCase("Jdbc")) {
                        return f.acceptsURL(url);
                    } else {
                        return f.dialectIdentifier().equalsIgnoreCase(dbType);
                    }
                }).collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            return new CommonDialect();
        }
        if (matchingFactories.size() > 1) {
            throw new JdbcConnectorException(
                    String.format(
                            "Multiple jdbc dialect factories can handle dbType '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            dbType,
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
            LOG.warn("Could not load service provider for jdbc dialects factory.", e);
            return new LinkedList<>();
        }
    }
}
