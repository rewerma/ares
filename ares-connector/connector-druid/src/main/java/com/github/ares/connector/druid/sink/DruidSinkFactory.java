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

package com.github.ares.connector.druid.sink;

import static com.github.ares.connector.druid.config.DruidOptions.URL;

import com.github.ares.api.table.connector.TableSink;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.api.table.factory.TableSinkFactoryContext;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.druid.config.DruidOptions;
import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class DruidSinkFactory implements TableSinkFactory {

    public static final String UNSUPPORTED =
            "Apache Druid SQL HTTP is read-only; INSERT/UPDATE/DELETE/MERGE and ingest writes are not supported";

    @Override
    public String factoryIdentifier() {
        return DruidOptions.CONNECTOR_IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().optional(URL).build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        throw new IllegalArgumentException(UNSUPPORTED);
    }
}
