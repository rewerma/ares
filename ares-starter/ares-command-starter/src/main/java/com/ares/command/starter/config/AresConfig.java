package com.ares.command.starter.config;

import lombok.Data;

import java.util.Properties;

@Data
public class AresConfig {
    public static volatile AresConfig ARES_CONFIG;

    private String aresHome;

    private String engineType;

    private String sparkHome;

    private Integer threadPoolSize;

    public static void init(Properties properties) {
        if (ARES_CONFIG == null) {
            synchronized (AresConfig.class) {
                if (ARES_CONFIG == null) {
                    ARES_CONFIG = new AresConfig();
                    ARES_CONFIG.aresHome = properties.getProperty("aresHome");
                    ARES_CONFIG.engineType = properties.getProperty("engineType");
                    ARES_CONFIG.sparkHome = properties.getProperty("sparkHome");
                    ARES_CONFIG.threadPoolSize = (Integer) properties.get("threadPoolSize");
                }
            }
        }
    }

    public static AresConfig getConfig() {
        return ARES_CONFIG;
    }
}
