/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.ares.connector.druid;

import com.github.ares.api.table.catalog.PhysicalColumn;
import com.github.ares.api.table.catalog.TableSchema;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.LocalTimeType;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class DruidTypeMapper {

    private DruidTypeMapper() {}

    public static TableSchema schema(List<String> columns, List<String> types) {
        return schema(columns, types, Collections.<Map<String, String>>emptyList());
    }

    public static TableSchema schema(List<String> columns, List<String> types, List<Map<String, String>> rows) {
        TableSchema.Builder builder = TableSchema.builder();
        if (columns == null) {
            return builder.build();
        }
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i);
            String type = types != null && i < types.size() ? types.get(i) : null;
            AresDataType<?> dataType = toAresType(name, type);
            if (isIntegral(dataType) && looksLikeTimestampColumn(name, rows)) {
                dataType = LocalTimeType.LOCAL_DATE_TIME_TYPE;
            }
            builder.column(PhysicalColumn.of(name, dataType, (Long) null, true, null, null));
        }
        return builder.build();
    }

    public static AresDataType<?> toAresType(String rawType) {
        return toAresType(null, rawType);
    }

    public static AresDataType<?> toAresType(String columnName, String rawType) {
        if (isTimeColumn(columnName)) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        }
        if (StringUtils.isBlank(rawType)) {
            return BasicType.STRING_TYPE;
        }
        String t = rawType.trim().toUpperCase();
        if (t.equals("LONG") || t.equals("BIGINT")) {
            return BasicType.LONG_TYPE;
        }
        if (t.equals("INT") || t.equals("INTEGER")) {
            return BasicType.INT_TYPE;
        }
        if (t.equals("DOUBLE")) {
            return BasicType.DOUBLE_TYPE;
        }
        if (t.equals("FLOAT")) {
            return BasicType.FLOAT_TYPE;
        }
        if (t.equals("BOOLEAN")) {
            return BasicType.BOOLEAN_TYPE;
        }
        if (t.equals("TIMESTAMP") || t.equals("DATETIME")) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        }
        if (t.equals("DATE")) {
            return LocalTimeType.LOCAL_DATE_TYPE;
        }
        if (t.startsWith("DECIMAL") || t.equals("NUMBER")) {
            return new DecimalType(38, 18);
        }
        return BasicType.STRING_TYPE;
    }

    public static Object convert(String value, AresDataType<?> dataType) {
        if (value == null || dataType == null) {
            return null;
        }
        if (dataType.equals(BasicType.LONG_TYPE)) {
            return Long.valueOf(value.trim());
        }
        if (dataType.equals(BasicType.INT_TYPE)) {
            return Integer.valueOf(value.trim());
        }
        if (dataType.equals(BasicType.DOUBLE_TYPE)) {
            return Double.valueOf(value.trim());
        }
        if (dataType.equals(BasicType.FLOAT_TYPE)) {
            return Float.valueOf(value.trim());
        }
        if (dataType.equals(BasicType.BOOLEAN_TYPE)) {
            return Boolean.valueOf(value.trim());
        }
        if (dataType.equals(LocalTimeType.LOCAL_DATE_TIME_TYPE)) {
            return DruidTimeValues.toLocalDateTime(value);
        }
        if (dataType.equals(LocalTimeType.LOCAL_DATE_TYPE)) {
            return DruidTimeValues.toLocalDate(value);
        }
        if (dataType instanceof DecimalType) {
            return new java.math.BigDecimal(value.trim());
        }
        return value;
    }

    static boolean isTimeColumn(String columnName) {
        return StringUtils.isNotBlank(columnName) && "__time".equalsIgnoreCase(columnName.trim());
    }

    private static boolean isIntegral(AresDataType<?> dataType) {
        return dataType != null
                && (dataType.equals(BasicType.LONG_TYPE) || dataType.equals(BasicType.INT_TYPE));
    }

    static boolean looksLikeTimestampColumn(String columnName, List<Map<String, String>> rows) {
        if (isTimeColumn(columnName) || rows == null || rows.isEmpty() || StringUtils.isBlank(columnName)) {
            return isTimeColumn(columnName);
        }
        boolean sawIso = false;
        for (Map<String, String> row : rows) {
            if (row == null) {
                continue;
            }
            String value = row.get(columnName);
            if (value == null || value.trim().isEmpty()) {
                continue;
            }
            if (DruidTimeValues.isIsoDateTime(value)) {
                sawIso = true;
                continue;
            }
            if (DruidTimeValues.isEpochNumber(value)) {
                continue;
            }
            return false;
        }
        return sawIso;
    }
}
