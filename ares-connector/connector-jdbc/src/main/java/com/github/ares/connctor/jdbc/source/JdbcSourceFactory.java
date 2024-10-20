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

package com.github.ares.connctor.jdbc.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SourceSplit;
import com.github.ares.api.source.TableSource;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.api.table.factory.TableSourceFactoryContext;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connctor.jdbc.config.JdbcSourceConfig;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectLoader;
import com.google.auto.service.AutoService;

import java.io.Serializable;

import static com.github.ares.connctor.jdbc.config.JdbcOptions.COMPATIBLE_MODE;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.DRIVER;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.FETCH_SIZE;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.PARTITION_COLUMN;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.PARTITION_LOWER_BOUND;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.PARTITION_NUM;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.PARTITION_UPPER_BOUND;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.PASSWORD;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.PROPERTIES;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.QUERY;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.URL;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.USER;
import static com.github.ares.connctor.jdbc.config.JdbcSourceOptions.SPLIT_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static com.github.ares.connctor.jdbc.config.JdbcSourceOptions.SPLIT_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static com.github.ares.connctor.jdbc.config.JdbcSourceOptions.SPLIT_INVERSE_SAMPLING_RATE;
import static com.github.ares.connctor.jdbc.config.JdbcSourceOptions.SPLIT_SAMPLE_SHARDING_THRESHOLD;
import static com.github.ares.connctor.jdbc.config.JdbcSourceOptions.SPLIT_SIZE;
import static com.github.ares.connctor.jdbc.config.JdbcSourceOptions.TABLE_LIST;
import static com.github.ares.connctor.jdbc.config.JdbcSourceOptions.TABLE_NAME;
import static com.github.ares.connctor.jdbc.config.JdbcSourceOptions.WHERE_CONDITION;


@AutoService(Factory.class)
public class JdbcSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Jdbc";
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
    TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        JdbcSourceConfig config = JdbcSourceConfig.of(context.getOptions());
        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load(
                        config.getJdbcConnectionConfig().getUrl(),
                        config.getJdbcConnectionConfig().getCompatibleMode());
        jdbcDialect.connectionUrlParse(
                config.getJdbcConnectionConfig().getUrl(),
                config.getJdbcConnectionConfig().getProperties(),
                jdbcDialect.defaultParameter());
        return () -> (AresSource<T, SplitT, StateT>) new JdbcSource(config);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, DRIVER)
                .optional(
                        USER,
                        PASSWORD,
                        CONNECTION_CHECK_TIMEOUT_SEC,
                        FETCH_SIZE,
                        PARTITION_COLUMN,
                        PARTITION_UPPER_BOUND,
                        PARTITION_LOWER_BOUND,
                        PARTITION_NUM,
                        COMPATIBLE_MODE,
                        PROPERTIES,
                        QUERY,
                        TABLE_NAME,
                        WHERE_CONDITION,
                        TABLE_LIST,
                        SPLIT_SIZE,
                        SPLIT_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        SPLIT_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        SPLIT_SAMPLE_SHARDING_THRESHOLD,
                        SPLIT_INVERSE_SAMPLING_RATE)
                .build();
    }

    @Override
    public Class<? extends AresSource> getSourceClass() {
        return JdbcSource.class;
    }
}
