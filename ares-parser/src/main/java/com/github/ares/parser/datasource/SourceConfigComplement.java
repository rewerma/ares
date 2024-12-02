package com.github.ares.parser.datasource;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface SourceConfigComplement {

    Map<String, String>  completeSourceConf(String datasource);

    Map<String, String> completeSourceConf(String datasource, Properties properties);

    List<String> completeSourceColumns(String datasource, String tableName, String sql);
}
