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

package com.github.ares.connctor.jdbc.sink;

import com.github.ares.api.table.catalog.CatalogOptions;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.connector.TableSink;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.api.table.factory.TableSinkFactoryContext;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connctor.jdbc.config.JdbcOptions;
import com.github.ares.connctor.jdbc.config.JdbcSinkConfig;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectLoader;
import com.github.ares.connctor.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.github.ares.connctor.jdbc.config.JdbcOptions.AUTO_COMMIT;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.BATCH_SIZE;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.COMPATIBLE_MODE;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.DATABASE;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.DRIVER;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.GENERATE_SINK_SQL;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.IS_EXACTLY_ONCE;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.MAX_COMMIT_ATTEMPTS;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.MAX_RETRIES;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.PASSWORD;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.PRIMARY_KEYS;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.QUERY;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.SUPPORT_UPSERT_BY_QUERY_PRIMARY_KEY_EXIST;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.TRANSACTION_TIMEOUT_SEC;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.URL;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.USER;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.XA_DATA_SOURCE_CLASS_NAME;

@AutoService(Factory.class)
public class JdbcSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Jdbc";
    }

    private ReadonlyConfig getCatalogOptions(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        // TODO Remove obsolete code
        Optional<Map<String, String>> catalogOptions =
                config.getOptional(CatalogOptions.CATALOG_OPTIONS);
        if (catalogOptions.isPresent()) {
            return ReadonlyConfig.fromMap(new HashMap<>(catalogOptions.get()));
        }
        return config;
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();

        List<Column> columns;
        if (catalogTable != null) {
            columns = catalogTable.getTableSchema().getColumns();
        } else {
            columns = new ArrayList<>();
        }

        // always execute
        final ReadonlyConfig options = config;
        JdbcSinkConfig sinkConfig = JdbcSinkConfig.of(config, columns);
        FieldIdeEnum fieldIdeEnum = config.get(JdbcOptions.FIELD_IDE);
        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        sinkConfig.getJdbcConnectionConfig().getUrl(),
                        sinkConfig.getJdbcConnectionConfig().getCompatibleMode(),
                        fieldIdeEnum == null ? null : fieldIdeEnum.getValue());
        dialect.connectionUrlParse(
                sinkConfig.getJdbcConnectionConfig().getUrl(),
                sinkConfig.getJdbcConnectionConfig().getProperties(),
                dialect.defaultParameter());
        final CatalogTable finalCatalogTable = catalogTable;
        if (finalCatalogTable == null) {
            return () -> new JdbcSink(options, sinkConfig, dialect, null);
        }
        return () ->
                new JdbcSink(options, sinkConfig, dialect, finalCatalogTable.getTableSchema().toPhysicalRowDataType());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, DRIVER)
                .optional(
                        USER,
                        PASSWORD,
                        CONNECTION_CHECK_TIMEOUT_SEC,
                        BATCH_SIZE,
                        IS_EXACTLY_ONCE,
                        GENERATE_SINK_SQL,
                        AUTO_COMMIT,
                        SUPPORT_UPSERT_BY_QUERY_PRIMARY_KEY_EXIST,
                        PRIMARY_KEYS,
                        COMPATIBLE_MODE)
                .conditional(
                        IS_EXACTLY_ONCE,
                        true,
                        XA_DATA_SOURCE_CLASS_NAME,
                        MAX_COMMIT_ATTEMPTS,
                        TRANSACTION_TIMEOUT_SEC)
                .conditional(IS_EXACTLY_ONCE, false, MAX_RETRIES)
                .conditional(GENERATE_SINK_SQL, true, DATABASE)
                .conditional(GENERATE_SINK_SQL, false, QUERY)
                // .conditional(DATA_SAVE_MODE, DataSaveMode.CUSTOM_PROCESSING, CUSTOM_SQL)
                .build();
    }
}
