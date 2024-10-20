package com.github.ares.parser.datasource;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface SourceConfigPatcher {

    Map<String, String> patchSourceConf(String datasource);

    Map<String, String> patchSourceConf(String datasource, Properties properties);

    List<String> patchColumns(String datasource, String tableName, String sql);
}
