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


import com.github.ares.api.source.SourceSplit;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.type.AresDataType;

public class JdbcSourceSplit implements SourceSplit {
    private final TablePath tablePath;
    private final String splitId;
    private final String splitQuery;
    private final String splitKeyName;
    private final AresDataType splitKeyType;
    private final Object splitStart;
    private final Object splitEnd;

    public JdbcSourceSplit(TablePath tablePath, String splitId, String splitQuery, String splitKeyName,
                           AresDataType splitKeyType, Object splitStart, Object splitEnd) {
        this.tablePath = tablePath;
        this.splitId = splitId;
        this.splitQuery = splitQuery;
        this.splitKeyName = splitKeyName;
        this.splitKeyType = splitKeyType;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public String getSplitId() {
        return splitId;
    }

    public String getSplitQuery() {
        return splitQuery;
    }

    public String getSplitKeyName() {
        return splitKeyName;
    }

    public AresDataType getSplitKeyType() {
        return splitKeyType;
    }

    public Object getSplitStart() {
        return splitStart;
    }

    public Object getSplitEnd() {
        return splitEnd;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "JdbcSourceSplit{" +
                "tablePath=" + tablePath +
                ", splitId='" + splitId + '\'' +
                ", splitQuery='" + splitQuery + '\'' +
                ", splitKeyName='" + splitKeyName + '\'' +
                ", splitKeyType=" + splitKeyType +
                ", splitStart=" + splitStart +
                ", splitEnd=" + splitEnd +
                '}';
    }
}
