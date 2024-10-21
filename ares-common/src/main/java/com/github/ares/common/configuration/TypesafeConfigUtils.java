package com.github.ares.common.configuration;

import com.github.ares.com.typesafe.config.Config;
import com.github.ares.com.typesafe.config.ConfigValue;
import lombok.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TypesafeConfigUtils {

    private TypesafeConfigUtils() {}

    /**
     * Check if config with specific prefix exists
     *
     * @param source config source
     * @param prefix config prefix
     * @return true if it has sub config
     */
    public static boolean hasSubConfig(Config source, String prefix) {

        boolean hasConfig = false;

        for (Map.Entry<String, ConfigValue> entry : source.entrySet()) {
            final String key = entry.getKey();

            if (key.startsWith(prefix)) {
                hasConfig = true;
                break;
            }
        }

        return hasConfig;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getConfig(
            final Config config, final String configKey, final T defaultValue) {
        if (!config.hasPath(configKey) && defaultValue == null) {
            return defaultValue;
        }
        if (defaultValue.getClass().equals(Long.class)) {
            return config.hasPath(configKey)
                    ? (T) Long.valueOf(config.getString(configKey))
                    : defaultValue;
        }
        if (defaultValue.getClass().equals(Integer.class)) {
            return config.hasPath(configKey)
                    ? (T) Integer.valueOf(config.getString(configKey))
                    : defaultValue;
        }
        if (defaultValue.getClass().equals(String.class)) {
            return config.hasPath(configKey) ? (T) config.getString(configKey) : defaultValue;
        }
        if (defaultValue.getClass().equals(Boolean.class)) {
            return config.hasPath(configKey)
                    ? (T) Boolean.valueOf(config.getString(configKey))
                    : defaultValue;
        }
        throw new RuntimeException("Unsupported config type, configKey: " + configKey);
    }

    public static List<? extends Config> getConfigList(
            Config config, String configKey, @NonNull List<? extends Config> defaultValue) {
        return config.hasPath(configKey) ? config.getConfigList(configKey) : defaultValue;
    }

    public static Map<String, String> configToMap(Config config) {
        Map<String, String> configMap = new HashMap<>();
        config.entrySet()
                .forEach(
                        entry -> {
                            configMap.put(entry.getKey(), entry.getValue().unwrapped().toString());
                        });
        return configMap;
    }
}
