package com.github.ares.connctor.jdbc.internal.converter;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.utils.JdbcFieldTypeUtils;
import com.github.ares.connctor.jdbc.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

/**
 * Base class for all converters that convert between JDBC object and Ares internal object.
 */
public abstract class AbstractJdbcRowConverter implements JdbcRowConverter {
    private static final Logger log = LoggerFactory.getLogger(AbstractJdbcRowConverter.class);

    public abstract String converterName();

    public AbstractJdbcRowConverter() {
    }

    @Override
    public AresRow toInternal(ResultSet rs, AresRowType typeInfo) throws SQLException {
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            AresDataType<?> aresDataType = typeInfo.getFieldType(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            switch (aresDataType.getSqlType()) {
                case STRING:
                    fields[fieldIndex] = JdbcUtils.getString(rs, resultSetIndex);
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
                            Optional.ofNullable(sqlDate).map(e -> e.toLocalDate()).orElse(null);
                    break;
                case TIME:
                    fields[fieldIndex] = readTime(rs, resultSetIndex);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = JdbcUtils.getTimestamp(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(e -> e.toLocalDateTime())
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
                    throw new AresException(
                            "Unexpected value: " + aresDataType);
            }
        }
        return new AresRow(fields);
    }

    protected LocalTime readTime(ResultSet rs, int resultSetIndex) throws SQLException {
        Time sqlTime = JdbcFieldTypeUtils.getTime(rs, resultSetIndex);
        return Optional.ofNullable(sqlTime).map(e -> e.toLocalTime()).orElse(null);
    }

    @Override
    public PreparedStatement toExternal(
            AresRowType rowType, AresRow row, PreparedStatement statement)
            throws SQLException {
        for (int fieldIndex = 0; fieldIndex < rowType.getTotalFields(); fieldIndex++) {
            AresDataType<?> aresDataType = rowType.getFieldType(fieldIndex);
            int statementIndex = fieldIndex + 1;
            Object fieldValue = row.getField(fieldIndex);
            if (fieldValue == null) {
                statement.setObject(statementIndex, null);
                continue;
            }

            switch (aresDataType.getSqlType()) {
                case STRING:
                    statement.setString(statementIndex, (String) row.getField(fieldIndex));
                    break;
                case BOOLEAN:
                    statement.setBoolean(statementIndex, (Boolean) row.getField(fieldIndex));
                    break;
                case TINYINT:
                    statement.setByte(statementIndex, (Byte) row.getField(fieldIndex));
                    break;
                case SMALLINT:
                    statement.setShort(statementIndex, (Short) row.getField(fieldIndex));
                    break;
                case INT:
                    statement.setInt(statementIndex, (Integer) row.getField(fieldIndex));
                    break;
                case BIGINT:
                    statement.setLong(statementIndex, (Long) row.getField(fieldIndex));
                    break;
                case FLOAT:
                    statement.setFloat(statementIndex, (Float) row.getField(fieldIndex));
                    break;
                case DOUBLE:
                    statement.setDouble(statementIndex, (Double) row.getField(fieldIndex));
                    break;
                case DECIMAL:
                    statement.setBigDecimal(statementIndex, (BigDecimal) row.getField(fieldIndex));
                    break;
                case DATE:
                    LocalDate localDate = (LocalDate) row.getField(fieldIndex);
                    statement.setDate(statementIndex, Date.valueOf(localDate));
                    break;
                case TIME:
                    LocalTime localTime = (LocalTime) row.getField(fieldIndex);
                    statement.setTime(statementIndex, Time.valueOf(localTime));
                    break;
                case TIMESTAMP:
                    LocalDateTime localDateTime = (LocalDateTime) row.getField(fieldIndex);
                    statement.setTimestamp(
                            statementIndex, Timestamp.valueOf(localDateTime));
                    break;
                case BYTES:
                    statement.setBytes(statementIndex, (byte[]) row.getField(fieldIndex));
                    break;
                case NULL:
                    statement.setNull(statementIndex, java.sql.Types.NULL);
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new AresException(
                            "Unexpected value: " + aresDataType);
            }
        }
        return statement;
    }
}
