package com.github.ares.parser.datasource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PropertiesDataSourcePatcher implements SourceConfigPatcher {
    private final Properties properties;

    public PropertiesDataSourcePatcher(Properties properties) {
        this.properties = properties;
    }

    @Override
    public Map<String, String> patchSourceConf(String datasource) {
        return patchSourceConf(datasource, this.properties);
    }

    public Map<String, String> patchSourceConf(String datasource, Properties properties) {
        Map<String, String> result = new LinkedHashMap<>();
        String prefix = "datasource." + datasource + ".";
        properties.forEach((k, v) -> {
            String key = (String) k;
            if (key.startsWith(prefix)) {
                String resKey = key.substring(prefix.length());
                result.put(resKey, v == null ? null : String.valueOf(v));
            }
        });

        return result;
    }

    @Override
    public List<String> patchColumns(String datasource, String tableName, String sql) {
        return null;
    }
}
