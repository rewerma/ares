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

import java.io.Serializable;

/** Ares connector error code interface */
public interface AresErrorCode extends Serializable {
    /**
     * Get error code
     *
     * @return error code
     */
    String getCode();

    /**
     * Get error description
     *
     * @return error description
     */
    String getDescription();

    default String getErrorMessage() {
        return String.format("ErrorCode:[%s], ErrorDescription:[%s]", getCode(), getDescription());
    }
}
