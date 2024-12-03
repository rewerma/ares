package com.github.ares.web.config;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class BaseConfig {
    @Value("${network.interface.preferred:}")
    private String preferredNetworkInterface;

    @PostConstruct
    public void init() {
        if (StringUtils.isNotBlank(preferredNetworkInterface)) {
            System.setProperty("network.interface.preferred", preferredNetworkInterface);
        }
    }
}
