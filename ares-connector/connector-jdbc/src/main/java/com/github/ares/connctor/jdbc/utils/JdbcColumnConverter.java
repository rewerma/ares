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

package com.github.ares.connctor.jdbc.utils;

import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.PhysicalColumn;
import com.github.ares.api.table.catalog.PrimitiveByteArrayType;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.LocalTimeType;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TIME_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

public class JdbcColumnConverter {

    public static List<Column> convert(DatabaseMetaData metadata, TablePath tablePath)
            throws SQLException {
        ResultSet columnsResultSet =
                metadata.getColumns(
                        tablePath.getDatabaseName(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName(),
                        null);

        List<Column> columns = new ArrayList<>();
        while (columnsResultSet.next()) {
            String columnName = columnsResultSet.getString("COLUMN_NAME");
            int jdbcType = columnsResultSet.getInt("DATA_TYPE");
            String nativeType = columnsResultSet.getString("TYPE_NAME");
            int columnSize = columnsResultSet.getInt("COLUMN_SIZE");
            int decimalDigits = columnsResultSet.getInt("DECIMAL_DIGITS");
            int nullable = columnsResultSet.getInt("NULLABLE");

            Column column =
                    convert(columnName, jdbcType, nativeType, nullable, columnSize, decimalDigits);
            columns.add(column);
        }
        return columns;
    }

    public static Column convert(ResultSetMetaData metadata, int index) throws SQLException {
        String columnName = metadata.getColumnLabel(index);
        int jdbcType = metadata.getColumnType(index);
        String nativeType = metadata.getColumnTypeName(index);
        int isNullable = metadata.isNullable(index);
        int precision = metadata.getPrecision(index);
        int scale = metadata.getScale(index);
        return convert(columnName, jdbcType, nativeType, isNullable, precision, scale);
    }

    public static Column convert(
            String columnName,
            int jdbcType,
            String nativeType,
            int isNullable,
            int precision,
            int scale)
            throws SQLException {
        int columnLength = precision;
        long longColumnLength = precision;
        long bitLength = 0;
        AresDataType aresType;

        switch (jdbcType) {
            case BOOLEAN:
                aresType = BasicType.BOOLEAN_TYPE;
                break;
            case BIT:
                if (precision == 1) {
                    aresType = BasicType.BOOLEAN_TYPE;
                } else {
                    aresType = PrimitiveByteArrayType.INSTANCE;
                }
                break;
            case TINYINT:
                aresType = BasicType.BYTE_TYPE;
                break;
            case SMALLINT:
                aresType = BasicType.SHORT_TYPE;
                break;
            case INTEGER:
                aresType = BasicType.INT_TYPE;
                break;
            case BIGINT:
                aresType = BasicType.LONG_TYPE;
                break;
            case FLOAT:
                aresType = BasicType.FLOAT_TYPE;
                break;
            case REAL:
                aresType = BasicType.DOUBLE_TYPE;
                break;
            case DOUBLE:
                aresType = BasicType.DOUBLE_TYPE;
                break;
            case NUMERIC:
            case DECIMAL:
                if (scale == 0) {
                    aresType = BasicType.LONG_TYPE;
                } else {
                    aresType = new DecimalType(precision, scale);
                }
                break;
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
            case CLOB:
            case NCLOB:
                aresType = BasicType.STRING_TYPE;
                columnLength = precision * 3;
                longColumnLength = precision * 3;
                break;
            case DATE:
                aresType = LocalTimeType.LOCAL_DATE_TYPE;
                break;
            case TIME:
            case TIME_WITH_TIMEZONE:
                aresType = LocalTimeType.LOCAL_TIME_TYPE;
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIMEZONE:
                aresType = LocalTimeType.LOCAL_DATE_TIME_TYPE;
                break;
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case BLOB:
                aresType = PrimitiveByteArrayType.INSTANCE;
                bitLength = precision * 8;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported JDBC type: " + jdbcType);
        }

        return PhysicalColumn.of(
                columnName,
                aresType,
                columnLength,
                isNullable != ResultSetMetaData.columnNoNulls,
                null,
                null,
                nativeType,
                false,
                false,
                bitLength,
                Collections.emptyMap(),
                longColumnLength);
    }
}
