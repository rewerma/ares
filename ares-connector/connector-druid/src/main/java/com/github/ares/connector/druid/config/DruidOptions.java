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

package com.github.ares.connector.druid.config;

import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.Options;

public final class DruidOptions {

    public static final String CONNECTOR_IDENTIFIER = "druid";

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Apache Druid SQL HTTP endpoint, e.g. http://host:8888/druid/v2/sql/");

    public static final Option<String> USER =
            Options.key("user").stringType().noDefaultValue().withDescription("Basic auth user");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Basic auth password");

    public static final Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Druid datasource / table name");

    public static final Option<String> QUERY =
            Options.key("query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional read-only SELECT or EXPLAIN SELECT");

    public static final Option<Integer> QUERY_TIMEOUT_MS =
            Options.key("query_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("HTTP query timeout in milliseconds");

    private DruidOptions() {}
}
