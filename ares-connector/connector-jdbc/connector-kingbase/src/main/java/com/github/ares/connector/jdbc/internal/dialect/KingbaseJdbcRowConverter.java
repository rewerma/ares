package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.connector.jdbc.exception.JdbcConnectorException;
import com.github.ares.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import com.github.ares.connector.jdbc.utils.JdbcUtils;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Locale;
import java.util.Optional;

public class KingbaseJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final String KB_GEOMETRY = "GEOMETRY";
    private static final String KB_GEOGRAPHY = "GEOGRAPHY";

    @Override
    public String converterName() {
        return DatabaseIdentifier.KINGBASE;
    }

    @Override
    public AresRow toInternal(ResultSet rs, AresRowType typeInfo) throws SQLException {
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            AresDataType<?> aresDataType = typeInfo.getFieldType(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            switch (aresDataType.getSqlType()) {
                case STRING:
                    fields[fieldIndex] = readString(rs, resultSetIndex);
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = JdbcUtils.getBoolean(rs, resultSetIndex);
                    break;
                case TINYINT:
                    fields[fieldIndex] = JdbcUtils.getByte(rs, resultSetIndex);
                    break;
                case SMALLINT:
                    fields[fieldIndex] = JdbcUtils.getShort(rs, resultSetIndex);
                    break;
                case INT:
                    fields[fieldIndex] = JdbcUtils.getInt(rs, resultSetIndex);
                    break;
                case BIGINT:
                    fields[fieldIndex] = JdbcUtils.getLong(rs, resultSetIndex);
                    break;
                case FLOAT:
                    fields[fieldIndex] = JdbcUtils.getFloat(rs, resultSetIndex);
                    break;
                case DOUBLE:
                    fields[fieldIndex] = JdbcUtils.getDouble(rs, resultSetIndex);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = JdbcUtils.getBigDecimal(rs, resultSetIndex);
                    break;
                case DATE:
                    Date sqlDate = JdbcUtils.getDate(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlDate).map(Date::toLocalDate).orElse(null);
                    break;
                case TIME:
                    fields[fieldIndex] = readTime(rs, resultSetIndex);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = JdbcUtils.getTimestamp(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(Timestamp::toLocalDateTime)
                                    .orElse(null);
                    break;
                case BYTES:
                    fields[fieldIndex] = JdbcUtils.getBytes(rs, resultSetIndex);
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

    private static String readString(ResultSet rs, int resultSetIndex) throws SQLException {
        String columnType = rs.getMetaData().getColumnTypeName(resultSetIndex);
        if (columnType != null) {
            String normalized = columnType.toUpperCase(Locale.ROOT);
            if (KB_GEOMETRY.equals(normalized) || KB_GEOGRAPHY.equals(normalized)) {
                Object value = rs.getObject(resultSetIndex);
                return value == null ? null : value.toString();
            }
        }
        return JdbcUtils.getString(rs, resultSetIndex);
    }
}
