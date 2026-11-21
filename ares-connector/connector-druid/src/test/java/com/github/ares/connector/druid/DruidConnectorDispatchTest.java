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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.github.ares.api.source.SourceTableInfo;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.FactoryUtil;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.connector.discovery.AresFactoryDiscovery;
import com.github.ares.connector.discovery.PluginIdentifier;
import com.github.ares.connector.druid.config.DruidOptions;
import com.github.ares.connector.druid.sink.DruidSinkFactory;
import com.github.ares.connector.utils.PluginUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Test;

public class DruidConnectorDispatchTest {

    private static final String ENDPOINT = "http://127.0.0.1:8888/druid/v2/sql/";

    @After
    public void clearTransport() {
        DruidSqlClient.setTestTransport(null);
    }

    @Test
    public void factoryDiscoveryFindsDruidSourceAndSink() {
        List<Factory> factories =
                FactoryUtil.discoverFactories(
                        Thread.currentThread().getContextClassLoader());
        boolean source = false;
        boolean sink = false;
        for (Factory factory : factories) {
            if (DruidOptions.CONNECTOR_IDENTIFIER.equalsIgnoreCase(factory.factoryIdentifier())) {
                if (factory instanceof TableSourceFactory) {
                    source = true;
                }
                if (factory instanceof TableSinkFactory) {
                    sink = true;
                }
            }
        }
        assertTrue("Druid TableSourceFactory must be on the ServiceLoader chain", source);
        assertTrue("Druid TableSinkFactory must be on the ServiceLoader chain", sink);
    }

    @Test
    public void pluginUtilCreateSourceRunsSelectThroughSqlHttp() {
        RecordingTransport transport =
                new RecordingTransport("[[\"__time\",\"page\"],[\"LONG\",\"STRING\"],[1690000000000,\"Home\"]]");
        DruidSqlClient.setTestTransport(transport);
        Map<String, Object> config = new LinkedHashMap<String, Object>();
        config.put("url", ENDPOINT);
        config.put("table_name", "docker-install-health-test");
        config.put("user", "admin");
        config.put("password", "secret");
        config.put("database", "druid");

        AresFactoryDiscovery discovery = new AresFactoryDiscovery(TableSourceFactory.class);
        SourceTableInfo info =
                PluginUtil.createSource(discovery, PluginIdentifier.of("source", "druid"), config);

        assertNotNull(info);
        assertEquals("druid", info.getSource().getPluginName());
        assertEquals("__time", info.getCatalogTables().get(0).getTableSchema().getColumns().get(0).getName());
        assertEquals(ENDPOINT, transport.lastUrl.get());
        assertEquals(
                "SELECT * FROM \"docker-install-health-test\"",
                queryFromRequestBody(transport.lastBody.get()));
        assertFalse(transport.lastUrl.get().matches(".*/druid/?$"));
        assertEquals("http://127.0.0.1:8888/druid/v2/sql/", transport.lastUrl.get());
        assertTrue(transport.lastHeaders.get().containsKey("Authorization"));
        assertFalse(String.valueOf(transport.lastBody.get()).contains("secret"));
    }

    @Test
    public void pluginUtilCreateSourceConvertsIso8601Time() {
        RecordingTransport transport =
                new RecordingTransport(
                        "[[\"__time\",\"id\",\"name\",\"c_time\"],[\"LONG\",\"LONG\",\"STRING\",\"STRING\"],[\"2023-03-09T12:23:00.000Z\",2,\"alice\",null]]");
        DruidSqlClient.setTestTransport(transport);
        Map<String, Object> config = new LinkedHashMap<String, Object>();
        config.put("url", ENDPOINT);
        config.put("table_name", "t_user1");
        AresFactoryDiscovery discovery = new AresFactoryDiscovery(TableSourceFactory.class);
        SourceTableInfo info =
                PluginUtil.createSource(discovery, PluginIdentifier.of("source", "druid"), config);
        assertEquals("__time", info.getCatalogTables().get(0).getTableSchema().getColumns().get(0).getName());
        assertEquals(
                com.github.ares.api.table.type.LocalTimeType.LOCAL_DATE_TIME_TYPE,
                info.getCatalogTables().get(0).getTableSchema().getColumns().get(0).getDataType());
        assertEquals(
                "SELECT * FROM \"t_user1\"",
                queryFromRequestBody(transport.lastBody.get()));
    }

    @Test
    public void pluginUtilCreateSourceAllowsExplain() {

        RecordingTransport transport = new RecordingTransport("[[\"PLAN\"],[\"STRING\"],[\"Scan\"]]");
        DruidSqlClient.setTestTransport(transport);
        Map<String, Object> config = new LinkedHashMap<String, Object>();
        config.put("url", ENDPOINT);
        config.put("query", "EXPLAIN SELECT 1");
        AresFactoryDiscovery discovery = new AresFactoryDiscovery(TableSourceFactory.class);
        SourceTableInfo info =
                PluginUtil.createSource(discovery, PluginIdentifier.of("source", "druid"), config);
        assertEquals("PLAN", info.getCatalogTables().get(0).getTableSchema().getColumns().get(0).getName());
        assertTrue(transport.lastBody.get().contains("EXPLAIN SELECT 1"));
    }

    @Test
    public void pluginUtilCreateSourceRejectsDmlWithoutHttp() {
        RecordingTransport transport = new RecordingTransport("[[\"c\"],[\"1\"]]");
        DruidSqlClient.setTestTransport(transport);
        Map<String, Object> config = new LinkedHashMap<String, Object>();
        config.put("url", ENDPOINT);
        config.put("query", "INSERT INTO foo VALUES (1)");
        AresFactoryDiscovery discovery = new AresFactoryDiscovery(TableSourceFactory.class);
        try {
            PluginUtil.createSource(discovery, PluginIdentifier.of("source", "druid"), config);
            fail("expected DML to be rejected");
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage().contains("read-only SELECT or EXPLAIN"));
            assertEquals(null, transport.lastBody.get());
        }
    }

    @Test
    public void sinkFactoryRejectsWrites() {
        AresFactoryDiscovery discovery = new AresFactoryDiscovery(TableSinkFactory.class);
        TableSinkFactory factory =
                (TableSinkFactory)
                        discovery.createOptionalPluginInstance(PluginIdentifier.of("sink", "druid"))
                                .get();
        try {
            factory.createSink(null);
            fail("expected sink to be rejected");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(DruidSinkFactory.UNSUPPORTED));
        }
    }

    @Test
    public void quoteHyphenatedAndQualifiedNames() {
        assertEquals(
                "\"docker-install-health-test\"",
                DruidIdentifiers.quoteTableName("docker-install-health-test"));
        assertEquals(
                "\"druid\".\"docker-install-health-test\"",
                DruidIdentifiers.quoteTableName("druid.docker-install-health-test"));
        assertEquals(
                "SELECT * FROM \"druid\".\"docker-install-health-test\"",
                DruidIdentifiers.selectStar("druid.docker-install-health-test"));
    }


    private static String queryFromRequestBody(String jsonBody) {
        if (jsonBody == null) {
            return null;
        }
        String key = "\"query\":";
        int at = jsonBody.indexOf(key);
        if (at < 0) {
            return null;
        }
        int i = at + key.length();
        while (i < jsonBody.length() && Character.isWhitespace(jsonBody.charAt(i))) {
            i++;
        }
        if (i >= jsonBody.length() || jsonBody.charAt(i) != '"') {
            return null;
        }
        StringBuilder out = new StringBuilder();
        i++;
        while (i < jsonBody.length()) {
            char c = jsonBody.charAt(i++);
            if (c == '\\') {
                if (i < jsonBody.length()) {
                    out.append(jsonBody.charAt(i++));
                }
                continue;
            }
            if (c == '"') {
                return out.toString();
            }
            out.append(c);
        }
        return out.toString();
    }

    private static final class RecordingTransport implements DruidHttpTransport {
        private final String response;
        private final AtomicReference<String> lastBody = new AtomicReference<String>();
        private final AtomicReference<String> lastUrl = new AtomicReference<String>();
        private final AtomicReference<Map<String, String>> lastHeaders =
                new AtomicReference<Map<String, String>>();

        private RecordingTransport(String response) {
            this.response = response;
        }

        @Override
        public DruidHttpResponse post(String url, Map<String, String> headers, String jsonBody, int timeoutMs) {
            lastUrl.set(url);
            lastBody.set(jsonBody);
            lastHeaders.set(headers);
            return new DruidHttpResponse(200, response);
        }
    }
}
