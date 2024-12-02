package com.github.ares.parser.datasource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PropertiesDataSourceComplement implements SourceConfigComplement {
    private final Properties properties;

    public PropertiesDataSourceComplement(Properties properties) {
        this.properties = properties;
    }

    @Override
    public Map<String, String> completeSourceConf(String datasource) {
        return completeSourceConf(datasource, this.properties);
    }

    @Override
    public Map<String, String> completeSourceConf(String datasource, Properties properties) {
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
    public List<String> completeSourceColumns(String datasource, String tableName, String sql) {
        return null;
    }
}
