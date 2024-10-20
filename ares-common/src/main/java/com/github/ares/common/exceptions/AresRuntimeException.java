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

import com.github.ares.com.fasterxml.jackson.core.type.TypeReference;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/** Ares global exception, used to tell user more clearly error messages */
public class AresRuntimeException extends RuntimeException {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final AresErrorCode aresErrorCode;
    private final Map<String, String> params;

    public AresRuntimeException(AresErrorCode aresErrorCode, String errorMessage) {
        super(aresErrorCode.getErrorMessage() + " - " + errorMessage);
        this.aresErrorCode = aresErrorCode;
        this.params = new HashMap<>();
        ExceptionParamsUtil.assertParamsMatchWithDescription(
                aresErrorCode.getDescription(), params);
    }

    public AresRuntimeException(
            AresErrorCode aresErrorCode, String errorMessage, Throwable cause) {
        super(aresErrorCode.getErrorMessage() + " - " + errorMessage, cause);
        this.aresErrorCode = aresErrorCode;
        this.params = new HashMap<>();
        ExceptionParamsUtil.assertParamsMatchWithDescription(
                aresErrorCode.getDescription(), params);
    }

    public AresRuntimeException(AresErrorCode aresErrorCode, Throwable cause) {
        super(aresErrorCode.getErrorMessage(), cause);
        this.aresErrorCode = aresErrorCode;
        this.params = new HashMap<>();
        ExceptionParamsUtil.assertParamsMatchWithDescription(
                aresErrorCode.getDescription(), params);
    }

    public AresRuntimeException(
            AresErrorCode aresErrorCode, Map<String, String> params) {
        super(ExceptionParamsUtil.getDescription(aresErrorCode.getErrorMessage(), params));
        this.aresErrorCode = aresErrorCode;
        this.params = params;
    }

    public AresRuntimeException(
            AresErrorCode aresErrorCode, Map<String, String> params, Throwable cause) {
        super(
                ExceptionParamsUtil.getDescription(aresErrorCode.getErrorMessage(), params),
                cause);
        this.aresErrorCode = aresErrorCode;
        this.params = params;
    }

    public AresErrorCode getAresErrorCode() {
        return aresErrorCode;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public Map<String, String> getParamsValueAsMap(String key) {
        try {
            return OBJECT_MAPPER.readValue(
                    params.get(key), new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T getParamsValueAs(String key) {
        try {
            return OBJECT_MAPPER.readValue(params.get(key), new TypeReference<T>() {});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
