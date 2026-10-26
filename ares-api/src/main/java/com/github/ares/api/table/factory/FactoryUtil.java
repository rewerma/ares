package com.github.ares.api.table.factory;

import com.github.ares.common.exceptions.AresException;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FactoryUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FactoryUtil.class);

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
