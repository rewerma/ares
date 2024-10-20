package com.github.ares.api.table.factory;

import com.github.ares.api.table.catalog.Catalog;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.AresException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

public class FactoryUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FactoryUtil.class);

    public static Optional<Catalog> createOptionalCatalog(
            String catalogName,
            ReadonlyConfig options,
            ClassLoader classLoader,
            String factoryIdentifier) {
        Optional<CatalogFactory> optionalFactory =
                discoverOptionalFactory(classLoader, CatalogFactory.class, factoryIdentifier);
        return optionalFactory.map(
                catalogFactory -> catalogFactory.createCatalog(catalogName, options));
    }

    public static <T extends Factory> Optional<T> discoverOptionalFactory(
            ClassLoader classLoader, Class<T> factoryClass, String factoryIdentifier) {
        final List<T> foundFactories = discoverFactories(classLoader, factoryClass);
        if (foundFactories.isEmpty()) {
            return Optional.empty();
        }
        final List<T> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equalsIgnoreCase(factoryIdentifier))
                        .collect(Collectors.toList());
        if (matchingFactories.isEmpty()) {
            return Optional.empty();
        }
        checkMultipleMatchingFactories(factoryIdentifier, factoryClass, matchingFactories);
        return Optional.of(matchingFactories.get(0));
    }

    public static <T extends Factory> List<T> discoverFactories(
            ClassLoader classLoader, Class<T> factoryClass) {
        return discoverFactories(classLoader).stream()
                .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                .map(f -> (T) f)
                .collect(Collectors.toList());
    }

    public static List<Factory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<Factory> result = new LinkedList<>();
            ServiceLoader.load(Factory.class, classLoader).iterator().forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for factories.", e);
            throw new AresException("Could not load service provider for factories.", e);
        }
    }

    private static <T extends Factory> void checkMultipleMatchingFactories(
            String factoryIdentifier, Class<T> factoryClass, List<T> matchingFactories) {
        if (matchingFactories.size() > 1) {
            throw new AresException(
                    String.format(
                            "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
    }
}
