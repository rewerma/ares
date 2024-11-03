package com.github.ares.common.configuration;

import com.github.ares.com.fasterxml.jackson.core.JsonProcessingException;
import com.github.ares.com.fasterxml.jackson.core.type.TypeReference;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.com.typesafe.config.ConfigFactory;
import com.github.ares.com.typesafe.config.ConfigRenderOptions;
import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.github.ares.common.configuration.utils.ConfigUtil.convertToJsonString;
import static com.github.ares.common.configuration.utils.ConfigUtil.convertValue;

public class ReadonlyConfig implements Serializable {
    private static final long serialVersionUID = -1L;
    /**
     * Stores the concrete key/value pairs of this configuration object.
     */
    @Getter
    protected final Map<String, Object> confData;

    private static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

    private ReadonlyConfig(Map<String, Object> confData) {
        this.confData = confData;
    }

    public static ReadonlyConfig fromMap(Map<String, Object> map) {
        return new ReadonlyConfig(map);
    }

    public <T> T get(Option<T> option) {
        return getOptional(option).orElseGet(option::defaultValue);
    }

    public Map<String, String> toMap() {
        if (confData.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> result = new LinkedHashMap<>();
        toMap(result);
        return result;
    }

    public void toMap(Map<String, String> result) {
        if (confData.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Object> entry : confData.entrySet()) {
            result.put(entry.getKey(), convertToJsonString(entry.getValue()));
        }
    }

    public <T> Optional<T> getOptional(Option<T> option) {
        if (option == null) {
            throw new NullPointerException("Option not be null.");
        }
        Object value = getValue(option.key());
        if (value == null) {
            for (String fallbackKey : option.getFallbackKeys()) {
                value = getValue(fallbackKey);
                if (value != null) {
                    break;
                }
            }
        }
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(convertValue(value, option));
    }

    private Object getValue(String key) {
        if (this.confData.containsKey(key)) {
            return this.confData.get(key);
        } else {
            String[] keys = key.split("\\.");
            Map<String, Object> data = this.confData;
            Object value = null;
            for (int i = 0; i < keys.length; i++) {
                value = data.get(keys[i]);
                if (i < keys.length - 1) {
                    if (!(value instanceof Map)) {
                        return null;
                    } else {
                        data = (Map<String, Object>) value;
                    }
                }
            }
            return value;
        }
    }

    public Config toConfig() {
        // TODO fix WHERE_CLAUSE object to config
        Map<String, Object> conf = new LinkedHashMap<>(confData);
        conf.remove("where_clause");
        return ConfigFactory.parseMap(conf);
    }

    public static ReadonlyConfig fromConfig(Config config) {
        try {
            return fromMap(
                    JACKSON_MAPPER.readValue(
                            config.root().render(ConfigRenderOptions.concise()),
                            new TypeReference<Map<String, Object>>() {}));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Json parsing exception.", e);
        }
    }

    @Override
    public int hashCode() {
        int hash = 0;
        for (String s : this.confData.keySet()) {
            hash ^= s.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ReadonlyConfig)) {
            return false;
        }
        Map<String, Object> otherConf = ((ReadonlyConfig) obj).confData;
        return this.confData.equals(otherConf);
    }

    @Override
    public String toString() {
        return convertToJsonString(this.confData);
    }
}
