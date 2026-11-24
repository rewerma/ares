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

import org.apache.commons.lang3.StringUtils;

/**
 * Quotes Apache Druid SQL identifiers. Hyphenated datasource names must be double-quoted. The HTTP
 * endpoint path is never derived from these names.
 */
public final class DruidIdentifiers {

    private DruidIdentifiers() {}

    public static String quoteIdentifier(String raw) {
        if (StringUtils.isBlank(raw)) {
            return raw;
        }
        String trimmed = raw.trim();
        if (trimmed.length() >= 2
                && trimmed.charAt(0) == '"'
                && trimmed.charAt(trimmed.length() - 1) == '"') {
            return trimmed;
        }
        return "\"" + trimmed.replace("\"", "\"\"") + "\"";
    }

    public static String quoteTableName(String tableName) {
        if (StringUtils.isBlank(tableName)) {
            return tableName;
        }
        String trimmed = tableName.trim();
        if (trimmed.length() >= 2
                && trimmed.charAt(0) == '"'
                && trimmed.charAt(trimmed.length() - 1) == '"') {
            return trimmed;
        }
        if (!trimmed.contains(".")) {
            return quoteIdentifier(trimmed);
        }
        String[] parts = trimmed.split("\\.");
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                out.append('.');
            }
            out.append(quoteIdentifier(parts[i]));
        }
        return out.toString();
    }

    public static String selectStar(String tableName) {
        return "SELECT * FROM " + quoteTableName(tableName);
    }

    public static boolean isReadOnlySql(String sql) {
        if (StringUtils.isBlank(sql)) {
            return false;
        }
        String trimmed = stripLeadingComments(sql).trim();
        if (trimmed.isEmpty()) {
            return false;
        }
        String head = trimmed.toUpperCase();
        if (head.startsWith("SELECT") || head.startsWith("WITH")) {
            return true;
        }
        return head.startsWith("EXPLAIN") && head.contains("SELECT");
    }

    private static String stripLeadingComments(String sql) {
        String s = sql.trim();
        while (true) {
            if (s.startsWith("--")) {
                int nl = s.indexOf('\n');
                s = nl < 0 ? "" : s.substring(nl + 1).trim();
                continue;
            }
            if (s.startsWith("/*")) {
                int end = s.indexOf("*/");
                s = end < 0 ? "" : s.substring(end + 2).trim();
                continue;
            }
            return s;
        }
    }
}
