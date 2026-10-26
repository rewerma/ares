package com.github.ares.test.spark;

import java.util.Properties;

public final class HiveTestUtils {

    private HiveTestUtils() {}

    public static boolean isHiveIntegrationEnabled() {
        String envFlag = System.getenv("HIVE_INTEGRATION_ENABLED");
        if ("true".equalsIgnoreCase(envFlag)) {
            return true;
        }
        Properties properties = Utils.loadProperties();
        return Boolean.parseBoolean(properties.getProperty("hive.integration.enabled", "false"));
    }
}
