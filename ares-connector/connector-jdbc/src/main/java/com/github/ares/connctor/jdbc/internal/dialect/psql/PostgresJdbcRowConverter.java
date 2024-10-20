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

package com.github.ares.connctor.jdbc.internal.dialect.psql;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.connctor.jdbc.exception.JdbcConnectorException;
import com.github.ares.connctor.jdbc.internal.converter.AbstractJdbcRowConverter;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Locale;
import java.util.Optional;

public class PostgresJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final String PG_GEOMETRY = "GEOMETRY";
    private static final String PG_GEOGRAPHY = "GEOGRAPHY";

    @Override
    public String converterName() {
        return null;
    }

    @Override
    @SuppressWarnings("checkstyle:Indentation")
    public AresRow toInternal(ResultSet rs, AresRowType typeInfo) throws SQLException {
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            AresDataType<?> aresDataType = typeInfo.getFieldType(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            String metaDataColumnType =
                    rs.getMetaData().getColumnTypeName(resultSetIndex).toUpperCase(Locale.ROOT);
            switch (aresDataType.getSqlType()) {
                case STRING:
                    if (metaDataColumnType.equals(PG_GEOMETRY)
                            || metaDataColumnType.equals(PG_GEOGRAPHY)) {
                        fields[fieldIndex] =
                                rs.getObject(resultSetIndex) == null
                                        ? null
                                        : rs.getObject(resultSetIndex).toString();
                    } else {
                        fields[fieldIndex] = rs.getString(resultSetIndex);
                    }
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = rs.getBoolean(resultSetIndex);
                    break;
                case TINYINT:
                    fields[fieldIndex] = rs.getByte(resultSetIndex);
                    break;
                case SMALLINT:
                    fields[fieldIndex] = rs.getShort(resultSetIndex);
                    break;
                case INT:
                    fields[fieldIndex] = rs.getInt(resultSetIndex);
                    break;
                case BIGINT:
                    fields[fieldIndex] = rs.getLong(resultSetIndex);
                    break;
                case FLOAT:
                    fields[fieldIndex] = rs.getFloat(resultSetIndex);
                    break;
                case DOUBLE:
                    fields[fieldIndex] = rs.getDouble(resultSetIndex);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = rs.getBigDecimal(resultSetIndex);
                    break;
                case DATE:
                    Date sqlDate = rs.getDate(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlDate).map(e -> e.toLocalDate()).orElse(null);
                    break;
                case TIME:
                    Time sqlTime = rs.getTime(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTime).map(e -> e.toLocalTime()).orElse(null);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = rs.getTimestamp(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(e -> e.toLocalDateTime())
                                    .orElse(null);
                    break;
                case BYTES:
                    fields[fieldIndex] = rs.getBytes(resultSetIndex);
                    break;
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new JdbcConnectorException("Unexpected value: " + aresDataType);
            }
        }
        return new AresRow(fields);
    }
}
