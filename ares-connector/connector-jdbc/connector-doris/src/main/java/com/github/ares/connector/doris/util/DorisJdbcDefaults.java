package com.github.ares.connector.doris.util;

import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.doris.config.DorisOptions;
import com.github.ares.connector.jdbc.config.JdbcOptions;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public final class DorisJdbcDefaults {
    private DorisJdbcDefaults() {}

    public static ReadonlyConfig apply(ReadonlyConfig options) {
        Map<String, Object> data = new LinkedHashMap<>(options.getConfData());
        Object driver = data.get(JdbcOptions.DRIVER.key());
        if (driver == null || StringUtils.isBlank(String.valueOf(driver))) {
            data.put(JdbcOptions.DRIVER.key(), DorisOptions.MYSQL_DRIVER);
        }
        return ReadonlyConfig.fromMap(data);
    }
}
