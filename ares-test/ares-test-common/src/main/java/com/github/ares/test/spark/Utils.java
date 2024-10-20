package com.github.ares.test.spark;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Utils {
    private static Properties configProperties = null;

    public static Properties loadProperties() {
        if (configProperties == null) {
            configProperties = loadProperties("config.properties");
        }
        return configProperties;
    }

    public static Properties loadProperties(String fileName) {
        Properties properties = new Properties();

        try (InputStream input = ClassLoader.getSystemClassLoader().getResourceAsStream(fileName)) {
            properties.load(input);
            return properties;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties from file: " + fileName, e);
        }
    }

    public static String getSparkMaster() {
        Properties properties = loadProperties();
        return properties.getProperty("spark.master");
    }
}
