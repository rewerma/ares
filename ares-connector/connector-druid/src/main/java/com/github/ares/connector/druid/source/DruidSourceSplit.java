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

import com.github.ares.api.source.SourceSplit;

public class DruidSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final String id;
    private final String sql;

    public DruidSourceSplit(String id, String sql) {
        this.id = id;
        this.sql = sql;
    }

    @Override
    public String splitId() {
        return id;
    }

    public String getSql() {
        return sql;
    }
}
