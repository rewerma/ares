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

package com.github.ares.common.exceptions;


import com.github.ares.com.fasterxml.jackson.core.JsonProcessingException;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

import static com.github.ares.common.exceptions.CommonErrorCode.CONVERT_TO_CONNECTOR_TYPE_ERROR_SIMPLE;
import static com.github.ares.common.exceptions.CommonErrorCode.CONVERT_TO_ARES_TYPE_ERROR_SIMPLE;
import static com.github.ares.common.exceptions.CommonErrorCode.FILE_NOT_EXISTED;
import static com.github.ares.common.exceptions.CommonErrorCode.FILE_OPERATION_FAILED;
import static com.github.ares.common.exceptions.CommonErrorCode.GET_CATALOG_TABLES_WITH_UNSUPPORTED_TYPE_ERROR;
import static com.github.ares.common.exceptions.CommonErrorCode.GET_CATALOG_TABLE_WITH_UNSUPPORTED_TYPE_ERROR;
import static com.github.ares.common.exceptions.CommonErrorCode.JSON_OPERATION_FAILED;
import static com.github.ares.common.exceptions.CommonErrorCode.UNSUPPORTED_DATA_TYPE;
import static com.github.ares.common.exceptions.CommonErrorCode.WRITE_ARES_ROW_ERROR;

public class CommonError {
    private static final String KEY_IDENTIFIER = "identifier";
    private static final String KEY_OPERATION = "operation";
    private static final String KEY_FILE_NAME = "fileName";
    private static final String KEY_DATA_TYPE = "dataType";
    private static final String KEY_FIELD = "field";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static AresRuntimeException fileOperationFailed(
            String identifier, String operation, String fileName, Throwable cause) {
        Map<String, String> params = new HashMap<>();
        params.put(KEY_IDENTIFIER, identifier);
        params.put(KEY_OPERATION, operation);
        params.put(KEY_FILE_NAME, fileName);
        return new AresRuntimeException(FILE_OPERATION_FAILED, params, cause);
    }

    public static AresRuntimeException fileOperationFailed(
            String identifier, String operation, String fileName) {
        Map<String, String> params = new HashMap<>();
        params.put(KEY_IDENTIFIER, identifier);
        params.put(KEY_OPERATION, operation);
        params.put(KEY_FILE_NAME, fileName);
        return new AresRuntimeException(FILE_OPERATION_FAILED, params);
    }

    public static AresRuntimeException fileNotExistFailed(
            String identifier, String operation, String fileName) {
        Map<String, String> params = new HashMap<>();
        params.put(KEY_IDENTIFIER, identifier);
        params.put(KEY_OPERATION, operation);
        params.put(KEY_FILE_NAME, fileName);
        return new AresRuntimeException(FILE_NOT_EXISTED, params);
    }

    public static AresRuntimeException writeAresRowFailed(
            String connector, String row, Throwable cause) {
        Map<String, String> params = new HashMap<>();
        params.put("connector", connector);
        params.put("aresRow", row);
        return new AresRuntimeException(WRITE_ARES_ROW_ERROR, params, cause);
    }

    public static AresRuntimeException unsupportedDataType(
            String identifier, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put(KEY_IDENTIFIER, identifier);
        params.put(KEY_DATA_TYPE, dataType);
        params.put(KEY_FIELD, field);
        return new AresRuntimeException(UNSUPPORTED_DATA_TYPE, params);
    }

    public static AresRuntimeException convertToAresTypeError(
            String identifier, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put(KEY_IDENTIFIER, identifier);
        params.put(KEY_DATA_TYPE, dataType);
        params.put(KEY_FIELD, field);
        return new AresRuntimeException(CONVERT_TO_ARES_TYPE_ERROR_SIMPLE, params);
    }



    public static AresRuntimeException convertToConnectorTypeError(
            String identifier, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put(KEY_IDENTIFIER, identifier);
        params.put(KEY_DATA_TYPE, dataType);
        params.put(KEY_FIELD, field);
        return new AresRuntimeException(CONVERT_TO_CONNECTOR_TYPE_ERROR_SIMPLE, params);
    }

    public static AresRuntimeException getCatalogTableWithUnsupportedType(
            String catalogName, String tableName, Map<String, String> fieldWithDataTypes) {
        Map<String, String> params = new HashMap<>();
        params.put("catalogName", catalogName);
        params.put("tableName", tableName);
        try {
            params.put("fieldWithDataTypes", OBJECT_MAPPER.writeValueAsString(fieldWithDataTypes));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new AresRuntimeException(GET_CATALOG_TABLE_WITH_UNSUPPORTED_TYPE_ERROR, params);
    }

    public static AresRuntimeException getCatalogTablesWithUnsupportedType(
            String catalogName, Map<String, Map<String, String>> tableUnsupportedTypes) {
        Map<String, String> params = new HashMap<>();
        params.put("catalogName", catalogName);
        try {
            params.put(
                    "tableUnsupportedTypes",
                    OBJECT_MAPPER.writeValueAsString(tableUnsupportedTypes));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new AresRuntimeException(
                GET_CATALOG_TABLES_WITH_UNSUPPORTED_TYPE_ERROR, params);
    }

    public static AresRuntimeException jsonOperationError(String identifier, String payload) {
        return jsonOperationError(identifier, payload, null);
    }

    public static AresRuntimeException jsonOperationError(
            String identifier, String payload, Throwable cause) {
        Map<String, String> params = new HashMap<>();
        params.put(KEY_IDENTIFIER, identifier);
        params.put("payload", payload);
        AresErrorCode code = JSON_OPERATION_FAILED;

        if (cause != null) {
            return new AresRuntimeException(code, params, cause);
        } else {
            return new AresRuntimeException(code, params);
        }
    }
}
