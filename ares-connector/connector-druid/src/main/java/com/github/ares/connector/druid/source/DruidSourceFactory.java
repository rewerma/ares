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

package com.github.ares.connector.druid.source;

import static com.github.ares.connector.druid.config.DruidOptions.PASSWORD;
import static com.github.ares.connector.druid.config.DruidOptions.QUERY;
import static com.github.ares.connector.druid.config.DruidOptions.QUERY_TIMEOUT_MS;
import static com.github.ares.connector.druid.config.DruidOptions.TABLE_NAME;
import static com.github.ares.connector.druid.config.DruidOptions.URL;
import static com.github.ares.connector.druid.config.DruidOptions.USER;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SourceSplit;
import com.github.ares.api.source.TableSource;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.api.table.factory.TableSourceFactoryContext;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.druid.config.DruidOptions;
import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class DruidSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return DruidOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL)
                .optional(USER, PASSWORD, TABLE_NAME, QUERY, QUERY_TIMEOUT_MS)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (AresSource<T, SplitT, StateT>) new DruidSource(context.getOptions());
    }

    @Override
    public Class<? extends AresSource> getSourceClass() {
        return DruidSource.class;
    }
}
