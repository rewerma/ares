package com.github.ares.connector.hive.utils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

@UtilityClass
public class HiveSchemaBuilder {

    public static Map<String, Object> buildSchema(Table table, String pluginName) {
        LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
        appendColumns(fields, table.getSd().getCols(), pluginName);
        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        if (partitionKeys != null && !partitionKeys.isEmpty()) {
            appendColumns(fields, partitionKeys, pluginName);
        }
        LinkedHashMap<String, Object> schema = new LinkedHashMap<>();
        schema.put("fields", fields);
        return schema;
    }

    private static void appendColumns(
            LinkedHashMap<String, Object> fields, List<FieldSchema> columns, String pluginName) {
        if (columns == null) {
            return;
        }
        for (FieldSchema column : columns) {
            fields.put(
                    column.getName(),
                    HiveTypeConverter.convert(pluginName, column.getName(), column.getType()));
        }
    }
}
