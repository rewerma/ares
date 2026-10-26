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

package com.github.ares.connector.jdbc.source;

import com.github.ares.api.source.SourceSplit;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRowType;

public class JdbcSourceSplit implements SourceSplit {
    private final TablePath tablePath;
    private final String splitId;
    private final String splitQuery;
    private final AresRowType splitKey;
    private final Object[] splitStart;
    private final Object[] splitEnd;

    public JdbcSourceSplit(
            TablePath tablePath,
            String splitId,
            String splitQuery,
            AresRowType splitKey,
            Object[] splitStart,
            Object[] splitEnd) {
        this.tablePath = tablePath;
        this.splitId = splitId;
        this.splitQuery = splitQuery;
        this.splitKey = splitKey;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
    }

    public JdbcSourceSplit(
            TablePath tablePath,
            String splitId,
            String splitQuery,
            String splitKeyName,
            AresDataType splitKeyType,
            Object splitStart,
            Object splitEnd) {
        this(
                tablePath,
                splitId,
                splitQuery,
                splitKeyName == null
                        ? null
                        : new AresRowType(
                                new String[] {splitKeyName}, new AresDataType[] {splitKeyType}),
                splitStart == null ? null : new Object[] {splitStart},
                splitEnd == null ? null : new Object[] {splitEnd});
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

    public AresRowType getSplitKey() {
        return splitKey;
    }

    public String getSplitKeyName() {
        return splitKey == null || splitKey.getTotalFields() == 0
                ? null
                : splitKey.getFieldNames()[0];
    }

    public AresDataType getSplitKeyType() {
        return splitKey == null || splitKey.getTotalFields() == 0 ? null : splitKey.getFieldType(0);
    }

    public Object[] getSplitStart() {
        return splitStart;
    }

    public Object[] getSplitEnd() {
        return splitEnd;
    }

    public Object getSplitStartValue() {
        return splitStart == null || splitStart.length == 0 ? null : splitStart[0];
    }

    public Object getSplitEndValue() {
        return splitEnd == null || splitEnd.length == 0 ? null : splitEnd[0];
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "JdbcSourceSplit{"
                + "tablePath="
                + tablePath
                + ", splitId='"
                + splitId
                + '\''
                + ", splitQuery='"
                + splitQuery
                + '\''
                + ", splitKey="
                + splitKey
                + ", splitStart="
                + java.util.Arrays.toString(splitStart)
                + ", splitEnd="
                + java.util.Arrays.toString(splitEnd)
                + '}';
    }
}
