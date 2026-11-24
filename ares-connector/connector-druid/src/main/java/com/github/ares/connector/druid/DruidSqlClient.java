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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Official Apache Druid SQL HTTP client ({@code POST /druid/v2/sql}). Passwords are used only for
 * the Authorization header and are never logged. The endpoint is used as provided; callers must not
 * rewrite {@code /druid/v2/sql/} when a schema or database is selected.
 */
public final class DruidSqlClient {

    public static final int DEFAULT_TIMEOUT_MS = 30000;

    private static final ThreadLocal<DruidHttpTransport> TEST_TRANSPORT = new ThreadLocal<>();

    private final DruidHttpTransport transport;

    public DruidSqlClient() {
        this(null);
    }

    public DruidSqlClient(DruidHttpTransport transport) {
        DruidHttpTransport test = TEST_TRANSPORT.get();
        this.transport = transport != null ? transport : (test != null ? test : new JdkTransport());
    }

    public static void setTestTransport(DruidHttpTransport transport) {
        if (transport == null) {
            TEST_TRANSPORT.remove();
        } else {
            TEST_TRANSPORT.set(transport);
        }
    }

    public DruidSqlResult query(
            String endpoint, String user, String password, String sql, int timeoutMs)
            throws Exception {
        if (StringUtils.isBlank(endpoint)) {
            throw new IllegalArgumentException("Apache Druid SQL HTTP url is required");
        }
        if (StringUtils.isBlank(sql)) {
            throw new IllegalArgumentException("SQL is required");
        }
        if (!DruidIdentifiers.isReadOnlySql(sql)) {
            throw new IllegalArgumentException(
                    "Apache Druid only allows read-only SELECT or EXPLAIN SELECT");
        }
        String json = DruidJson.sqlRequestBody(sql);
        Map<String, String> headers = new LinkedHashMap<String, String>();
        putBasicAuth(headers, user, password);
        DruidHttpResponse response =
                transport.post(ensureTrailingSlash(endpoint.trim()), headers, json, timeoutMs);
        if (response.getStatus() < 200 || response.getStatus() >= 300) {
            throw new IllegalStateException(
                    "Apache Druid SQL request failed: HTTP " + response.getStatus());
        }
        return parseArrayResult(response.getBody());
    }

    static String ensureTrailingSlash(String url) {
        return url.endsWith("/") ? url : url + "/";
    }

    static void putBasicAuth(Map<String, String> headers, String user, String password) {
        String u = StringUtils.trimToNull(user);
        String p = StringUtils.trimToNull(password);
        if (u == null && p == null) {
            return;
        }
        String token = (u == null ? "" : u) + ":" + (p == null ? "" : p);
        String encoded = Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
        headers.put("Authorization", "Basic " + encoded);
    }

    static DruidSqlResult parseArrayResult(String body) {
        List<List<String>> table = DruidJson.parseArrayOfArrays(body);
        List<String> columns = new ArrayList<String>();
        List<String> types = new ArrayList<String>();
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
        if (table == null || table.isEmpty()) {
            return new DruidSqlResult(columns, types, rows);
        }
        List<String> header = table.get(0);
        if (header != null) {
            columns.addAll(header);
        }
        int dataStart = 1;
        if (table.size() > 1 && looksLikeTypesRow(table.get(1), columns.size())) {
            List<String> typeRow = table.get(1);
            for (int i = 0; i < columns.size(); i++) {
                String type = "";
                if (typeRow != null && i < typeRow.size() && typeRow.get(i) != null) {
                    type = typeRow.get(i);
                }
                types.add(type);
            }
            dataStart = 2;
        }
        for (int i = dataStart; i < table.size(); i++) {
            List<String> rowNode = table.get(i);
            Map<String, String> row = new LinkedHashMap<String, String>();
            for (int c = 0; c < columns.size(); c++) {
                String value = null;
                if (rowNode != null && c < rowNode.size()) {
                    value = rowNode.get(c);
                }
                row.put(columns.get(c), value);
            }
            rows.add(row);
        }
        return new DruidSqlResult(columns, types, rows);
    }

    private static boolean looksLikeTypesRow(List<String> row, int columnCount) {
        if (row == null || row.size() != columnCount) {
            return false;
        }
        for (String text : row) {
            if (text == null || text.isEmpty()) {
                continue;
            }
            if (!isTypeName(text)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isTypeName(String text) {
        String t = text.trim().toUpperCase();
        return t.equals("STRING")
                || t.equals("LONG")
                || t.equals("DOUBLE")
                || t.equals("FLOAT")
                || t.equals("BOOLEAN")
                || t.equals("TIMESTAMP")
                || t.equals("DATE")
                || t.equals("VARCHAR")
                || t.equals("BIGINT")
                || t.equals("INT")
                || t.equals("INTEGER")
                || t.equals("DECIMAL")
                || t.equals("COMPLEX")
                || t.startsWith("ARRAY")
                || t.contains("CHAR");
    }

    private static final class JdkTransport implements DruidHttpTransport {
        @Override
        public DruidHttpResponse post(
                String url, Map<String, String> headers, String jsonBody, int timeoutMs)
                throws Exception {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout(Math.max(timeoutMs, 1000));
            conn.setReadTimeout(Math.max(timeoutMs, 1000));
            conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
            if (headers != null) {
                for (Map.Entry<String, String> e : headers.entrySet()) {
                    conn.setRequestProperty(e.getKey(), e.getValue());
                }
            }
            byte[] payload = jsonBody.getBytes(StandardCharsets.UTF_8);
            conn.setFixedLengthStreamingMode(payload.length);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload);
            }
            int status = conn.getResponseCode();
            InputStream in = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
            String body = readStream(in);
            return new DruidHttpResponse(status, body);
        }

        private static String readStream(InputStream in) throws Exception {
            if (in == null) {
                return "";
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) >= 0) {
                out.write(buf, 0, n);
            }
            return new String(out.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    public static final class DruidSqlResult {
        private final List<String> columns;
        private final List<String> types;
        private final List<Map<String, String>> rows;

        public DruidSqlResult(
                List<String> columns, List<String> types, List<Map<String, String>> rows) {
            this.columns = columns == null ? Collections.<String>emptyList() : columns;
            this.types = types == null ? Collections.<String>emptyList() : types;
            this.rows = rows == null ? Collections.<Map<String, String>>emptyList() : rows;
        }

        public List<String> getColumns() {
            return columns;
        }

        public List<String> getTypes() {
            return types;
        }

        public List<Map<String, String>> getRows() {
            return rows;
        }
    }
}
