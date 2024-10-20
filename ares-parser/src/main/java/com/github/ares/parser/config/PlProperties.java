package com.github.ares.parser.config;

import java.io.Serializable;
import java.util.Properties;

public class PlProperties implements Serializable {
    private static final long serialVersionUID = 1L;

    private Properties properties;

    public void init(Properties properties) {
        this.properties = properties;
    }

    public Properties getProperties() {
        if (properties == null) {
            return new Properties();
        }
        return properties;
    }
}
