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

package com.github.ares.connector.jdbc.sink;

import static com.github.ares.connector.jdbc.config.JdbcOptions.AUTO_COMMIT;
import static com.github.ares.connector.jdbc.config.JdbcOptions.BATCH_SIZE;
import static com.github.ares.connector.jdbc.config.JdbcOptions.COMPATIBLE_MODE;
import static com.github.ares.connector.jdbc.config.JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC;
import static com.github.ares.connector.jdbc.config.JdbcOptions.CONNECTION_POOL_ENABLED;
import static com.github.ares.connector.jdbc.config.JdbcOptions.DRIVER;
import static com.github.ares.connector.jdbc.config.JdbcOptions.MAX_RETRIES;
import static com.github.ares.connector.jdbc.config.JdbcOptions.PASSWORD;
import static com.github.ares.connector.jdbc.config.JdbcOptions.POOL_SIZE;
import static com.github.ares.connector.jdbc.config.JdbcOptions.QUERY_TIMEOUT_SEC;
import static com.github.ares.connector.jdbc.config.JdbcOptions.URL;
import static com.github.ares.connector.jdbc.config.JdbcOptions.USER;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.connector.TableSink;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.api.table.factory.TableSinkFactoryContext;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.jdbc.config.JdbcOptions;
import com.github.ares.connector.jdbc.config.JdbcSinkConfig;
import com.github.ares.connector.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connector.jdbc.internal.dialect.JdbcDialectLoader;
import com.github.ares.connector.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import java.util.ArrayList;
import java.util.List;

public class JdbcSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Jdbc";
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

        JdbcSinkConfig sinkConfig = JdbcSinkConfig.of(config, columns);
        FieldIdeEnum fieldIdeEnum = config.get(JdbcOptions.FIELD_IDE);
        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        sinkConfig.getDbType(),
                        sinkConfig.getJdbcConnectionConfig().getUrl(),
                        sinkConfig.getJdbcConnectionConfig().getCompatibleMode(),
                        fieldIdeEnum == null ? null : fieldIdeEnum.getValue());
        dialect.connectionUrlParse(
                sinkConfig.getJdbcConnectionConfig().getUrl(),
                sinkConfig.getJdbcConnectionConfig().getProperties(),
                dialect.defaultParameter());
        final CatalogTable finalCatalogTable = catalogTable;
        if (finalCatalogTable == null) {
            return () -> new JdbcSink(sinkConfig, dialect, null);
        }
        return () ->
                new JdbcSink(
                        sinkConfig,
                        dialect,
                        finalCatalogTable.getTableSchema().toPhysicalRowDataType());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, DRIVER)
                .optional(
                        USER,
                        PASSWORD,
                        CONNECTION_CHECK_TIMEOUT_SEC,
                        CONNECTION_POOL_ENABLED,
                        POOL_SIZE,
                        QUERY_TIMEOUT_SEC,
                        BATCH_SIZE,
                        AUTO_COMMIT,
                        MAX_RETRIES,
                        COMPATIBLE_MODE)
                .build();
    }
}
