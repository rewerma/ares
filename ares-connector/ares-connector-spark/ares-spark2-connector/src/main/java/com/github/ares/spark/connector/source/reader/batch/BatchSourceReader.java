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

package com.github.ares.spark.connector.source.reader.batch;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SupportCoordinate;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.spark.connector.source.partition.batch.BatchPartition;
import com.github.ares.spark.connector.utils.TypeConverterUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BatchSourceReader implements DataSourceReader {

    protected final AresSource<AresRow, ?, ?> source;
    protected final Integer parallelism;
    private Map<String, String> envOptions;

    public BatchSourceReader(
            AresSource<AresRow, ?, ?> source,
            Integer parallelism,
            Map<String, String> envOptions) {
        this.source = source;
        this.parallelism = parallelism;
        this.envOptions = envOptions;
    }

    @Override
    public StructType readSchema() {
        return (StructType) TypeConverterUtils.convert(source.getProducedType());
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        List<InputPartition<InternalRow>> virtualPartitions;
        if (source instanceof SupportCoordinate) {
            virtualPartitions = new ArrayList<>(1);
            virtualPartitions.add(new BatchPartition(source, parallelism, 0, envOptions));
        } else {
            virtualPartitions = new ArrayList<>(parallelism);
            for (int subtaskId = 0; subtaskId < parallelism; subtaskId++) {
                virtualPartitions.add(
                        new BatchPartition(source, parallelism, subtaskId, envOptions));
            }
        }
        return virtualPartitions;
    }
}
