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

package com.github.ares.spark.connector.source;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.api.env.EnvCommonOptions;
import com.github.ares.api.source.AresSource;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.utils.Constants;
import com.github.ares.common.utils.SerializationUtils;
import com.github.ares.spark.connector.source.reader.batch.BatchSourceReader;
import com.github.ares.spark.connector.source.reader.micro.MicroBatchSourceReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class AresSourceSupport
        implements DataSourceV2, ReadSupport, MicroBatchReadSupport, DataSourceRegister {
    private static final Logger LOG = LoggerFactory.getLogger(AresSourceSupport.class);
    public static final String ARES_SOURCE_NAME = "AresSource";
    public static final Integer CHECKPOINT_INTERVAL_DEFAULT = 10000;

    @Override
    public String shortName() {
        return ARES_SOURCE_NAME;
    }

    @Override
    public DataSourceReader createReader(StructType rowType, DataSourceOptions options) {
        return createReader(options);
    }

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        AresSource<AresRow, ?, ?> aresSource = getAresSource(options);
        int parallelism = options.getInt(CommonOptions.PARALLELISM.key(), 1);
        Map<String, String> envOptions = options.asMap();
        return new BatchSourceReader(aresSource, parallelism, envOptions);
    }

    @Override
    public MicroBatchReader createMicroBatchReader(
            Optional<StructType> rowTypeOptional,
            String checkpointLocation,
            DataSourceOptions options) {
        AresSource<AresRow, ?, ?> aresSource = getAresSource(options);
        Integer parallelism = options.getInt(CommonOptions.PARALLELISM.key(), 1);
        Integer checkpointInterval =
                options.getInt(
                        EnvCommonOptions.CHECKPOINT_INTERVAL.key(), CHECKPOINT_INTERVAL_DEFAULT);
        String checkpointPath =
                StringUtils.replacePattern(checkpointLocation, "sources/\\d+", "sources-state");
        Configuration configuration =
                SparkSession.getActiveSession().get().sparkContext().hadoopConfiguration();
        String hdfsRoot =
                options.get(Constants.HDFS_ROOT)
                        .orElse(FileSystem.getDefaultUri(configuration).toString());
        String hdfsUser = options.get(Constants.HDFS_USER).orElse("");
        Integer checkpointId = options.getInt(Constants.CHECKPOINT_ID, 1);
        Map<String, String> envOptions = options.asMap();
        return new MicroBatchSourceReader(
                aresSource,
                parallelism,
                checkpointId,
                checkpointInterval,
                checkpointPath,
                hdfsRoot,
                hdfsUser,
                envOptions);
    }

    private AresSource<AresRow, ?, ?> getAresSource(DataSourceOptions options) {
        return SerializationUtils.stringToObject(
                options.get(Constants.SOURCE_SERIALIZATION)
                        .orElseThrow(
                                () ->
                                        new UnsupportedOperationException(
                                                "Serialization information for the AresSource is required")));
    }
}
