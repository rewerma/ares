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

package com.github.ares.spark.connector.sink;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.common.utils.Constants;
import com.github.ares.common.utils.SerializationUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;

public class SparkSinkInjector {

    private static final String SPARK_SINK_CLASS_NAME =
            "com.github.ares.spark.connector.sink.SparkSink";

    public static final String SINK_CATALOG_TABLE = "sink.catalog.table";

    public static DataStreamWriter<Row> inject(
            DataStreamWriter<Row> dataset,
            AresSink<?, ?, ?, ?> sink,
            CatalogTable catalogTable) {
        return dataset.format(SPARK_SINK_CLASS_NAME)
                .outputMode(OutputMode.Append())
                .option(Constants.SINK_SERIALIZATION, SerializationUtils.objectToString(sink))
                .option(SINK_CATALOG_TABLE, SerializationUtils.objectToString(catalogTable));
    }

    public static DataFrameWriter<Row> inject(
            DataFrameWriter<Row> dataset,
            AresSink<?, ?, ?, ?> sink,
            CatalogTable catalogTable) {
        return dataset.format(SPARK_SINK_CLASS_NAME)
                .option(Constants.SINK_SERIALIZATION, SerializationUtils.objectToString(sink))
                .option(SINK_CATALOG_TABLE, SerializationUtils.objectToString(catalogTable));
    }
}
