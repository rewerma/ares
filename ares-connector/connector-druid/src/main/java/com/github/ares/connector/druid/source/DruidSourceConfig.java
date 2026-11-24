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

import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.druid.DruidIdentifiers;
import java.io.Serializable;
import org.apache.commons.lang3.StringUtils;

/** Spark serializes the source (and this config) into DataFrameReader options. */
public class DruidSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String url;
    private final String user;
    private final String password;
    private final String tableName;
    private final String query;
    private final int timeoutMs;

    public DruidSourceConfig(ReadonlyConfig options) {
        this.url = options.get(URL);
        this.user = options.getOptional(USER).orElse(null);
        this.password = options.getOptional(PASSWORD).orElse(null);
        this.tableName = options.getOptional(TABLE_NAME).orElse(null);
        this.query = options.getOptional(QUERY).orElse(null);
        Integer timeout = options.get(QUERY_TIMEOUT_MS);
        this.timeoutMs = timeout == null ? 30000 : timeout.intValue();
        if (StringUtils.isBlank(url)) {
            throw new IllegalArgumentException("Apache Druid SQL HTTP url is required");
        }
        if (StringUtils.isBlank(query) && StringUtils.isBlank(tableName)) {
            throw new IllegalArgumentException(
                    "Apache Druid source requires table_name or a read-only query");
        }
        if (StringUtils.isNotBlank(query) && !DruidIdentifiers.isReadOnlySql(query)) {
            throw new IllegalArgumentException(
                    "Apache Druid only allows read-only SELECT or EXPLAIN SELECT");
        }
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getTableName() {
        return tableName;
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }

    public String resolveSql() {
        if (StringUtils.isNotBlank(query)) {
            return query.trim();
        }
        return DruidIdentifiers.selectStar(tableName);
    }
}
