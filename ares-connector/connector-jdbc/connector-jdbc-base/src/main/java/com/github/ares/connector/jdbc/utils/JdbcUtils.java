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
package com.github.ares.connector.jdbc.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

public final class JdbcUtils {

    private JdbcUtils() {
    }

    public static String getString(ResultSet resultSet, int columnIndex) throws SQLException {
        String value = resultSet.getString(columnIndex);
        if (value != null) {
            return value;
        }
        Clob clob = resultSet.getClob(columnIndex);
        if (clob == null) {
            return null;
        }
        try {
            long length = clob.length();
            if (length > Integer.MAX_VALUE) {
                throw new SQLException("CLOB length exceeds Integer.MAX_VALUE");
            }
            return clob.getSubString(1, (int) length);
        } finally {
            clob.free();
        }
    }

    public static Boolean getBoolean(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getBoolean(columnIndex);
    }

    public static Byte getByte(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getByte(columnIndex);
    }

    public static Short getShort(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getShort(columnIndex);
    }

    public static Integer getInt(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getInt(columnIndex);
    }

    public static Long getLong(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getLong(columnIndex);
    }

    public static Float getFloat(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getFloat(columnIndex);
    }

    public static Double getDouble(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getDouble(columnIndex);
    }

    public static BigDecimal getBigDecimal(ResultSet resultSet, int columnIndex)
            throws SQLException {
        return resultSet.getBigDecimal(columnIndex);
    }

    public static Date getDate(ResultSet resultSet, int columnIndex) throws SQLException {
        return resultSet.getDate(columnIndex);
    }

    public static Time getTime(ResultSet resultSet, int columnIndex) throws SQLException {
        return resultSet.getTime(columnIndex);
    }

    public static Timestamp getTimestamp(ResultSet resultSet, int columnIndex) throws SQLException {
        return resultSet.getTimestamp(columnIndex);
    }

    public static byte[] getBytes(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        byte[] bytes = resultSet.getBytes(columnIndex);
        if (bytes != null) {
            return bytes;
        }
        Blob blob = resultSet.getBlob(columnIndex);
        if (blob == null) {
            return null;
        }
        InputStream inputStream = null;
        try {
            inputStream = blob.getBinaryStream();
            return readAllBytes(inputStream);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    throw new SQLException("Failed to close BLOB stream", e);
                }
            }
            blob.free();
        }
    }

    private static byte[] readAllBytes(InputStream inputStream) throws SQLException {
        try {
            byte[] buffer = new byte[8192];
            int read;
            int offset = 0;
            byte[] data = new byte[0];
            while ((read = inputStream.read(buffer)) != -1) {
                byte[] next = new byte[offset + read];
                System.arraycopy(data, 0, next, 0, offset);
                System.arraycopy(buffer, 0, next, offset, read);
                data = next;
                offset += read;
            }
            return data;
        } catch (IOException e) {
            throw new SQLException("Failed to read binary stream", e);
        }
    }
}
