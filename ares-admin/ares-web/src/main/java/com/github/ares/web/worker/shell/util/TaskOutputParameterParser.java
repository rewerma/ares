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

package com.github.ares.web.worker.shell.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL;

@Slf4j
@Getter
public class TaskOutputParameterParser {
    private static final String RESULT_PREFIX = "[LAST_RESULT]:";

    private static final String FAILED_STATUS_PREFIX = "[ARES-FAILED] ";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
            .configure(READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
            .setSerializationInclusion(JsonInclude.Include.ALWAYS)
            .setDateFormat(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
            .setTimeZone(TimeZone.getDefault());

    private Map<String, Object> outputParams = new LinkedHashMap<>();

    private List<Map<String, Object>> lastResult = new ArrayList<>();

    private String errorMessage;

    public void appendParseLog(String logLine) {
        int resultIndex = logLine.indexOf(RESULT_PREFIX);
        if (resultIndex > -1) {
            String result = logLine.substring(resultIndex + RESULT_PREFIX.length());

            CollectionType listType =
                    OBJECT_MAPPER.getTypeFactory().constructCollectionType(ArrayList.class, LinkedHashMap.class);
            try {
                List<Map<String, Object>> resulList = OBJECT_MAPPER.readValue(result, listType);
                if (!resulList.isEmpty()) {
                    this.outputParams = resulList.get(0);
                }
                this.lastResult = resulList;
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        int statusIndex = logLine.indexOf(FAILED_STATUS_PREFIX);
        if (statusIndex > -1) {
            errorMessage = logLine.substring(statusIndex + FAILED_STATUS_PREFIX.length());
        }
    }
}
