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

package com.github.ares.core.starter.enums;

/** Engine type enum */
public enum EngineType {
    SPARK2("spark", "ares-spark2-starter.jar", "start-ares-spark2-connector.sh"),
    SPARK3("spark", "ares-spark3-starter.jar", "start-ares-spark3-connector.sh"),
    FLINK13("flink", "ares-flink13-starter.jar", "start-ares-flink13-connector.sh"),
    FLINK15("flink", "ares-flink15-starter.jar", "start-ares-flink15-connector.sh");

    private final String engine;
    private final String starterJarName;
    private final String starterShellName;

    EngineType(String engine, String starterJarName, String starterShellName) {
        this.engine = engine;
        this.starterJarName = starterJarName;
        this.starterShellName = starterShellName;
    }

    public String getEngine() {
        return engine;
    }

    public String getStarterJarName() {
        return starterJarName;
    }

    public String getStarterShellName() {
        return starterShellName;
    }
}
