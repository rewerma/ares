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

package com.github.ares.connctor.jdbc.sink;


import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionPoolManager {

    private final HikariDataSource connectionPool;

    private final Map<Integer, Connection> connectionMap;

    ConnectionPoolManager(HikariDataSource connectionPool) {
        this.connectionPool = connectionPool;
        connectionMap = new ConcurrentHashMap<>();
    }

    public Connection getConnection(int index) {
        return connectionMap.computeIfAbsent(
                index,
                i -> {
                    try {
                        return connectionPool.getConnection();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public boolean containsConnection(int index) {
        return connectionMap.containsKey(index);
    }

    public Connection remove(int index) {
        return connectionMap.remove(index);
    }

    public String getPoolName() {
        return connectionPool.getPoolName();
    }

    public void close() {
        if (!connectionPool.isClosed()) {
            connectionPool.close();
        }
    }

    public HikariDataSource getConnectionPool() {
        return connectionPool;
    }

    public Map<Integer, Connection> getConnectionMap() {
        return connectionMap;
    }
}
