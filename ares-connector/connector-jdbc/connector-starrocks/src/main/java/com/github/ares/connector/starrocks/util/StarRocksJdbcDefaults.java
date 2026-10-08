package com.github.ares.connector.starrocks.util;

import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.jdbc.config.JdbcOptions;
import com.github.ares.connector.starrocks.config.StarRocksOptions;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public final class StarRocksJdbcDefaults {
    private StarRocksJdbcDefaults() {}

    public static ReadonlyConfig apply(ReadonlyConfig options) {
        Map<String, Object> data = new LinkedHashMap<>(options.getConfData());
        Object driver = data.get(JdbcOptions.DRIVER.key());
        if (driver == null || StringUtils.isBlank(String.valueOf(driver))) {
            data.put(JdbcOptions.DRIVER.key(), StarRocksOptions.MYSQL_DRIVER);
        }
        return ReadonlyConfig.fromMap(data);
    }
}
