package com.github.ares.connctor.jdbc.internal.dialect.sqlserver;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.SqlType;
import com.github.ares.connctor.jdbc.exception.JdbcConnectorException;
import com.github.ares.connctor.jdbc.internal.converter.AbstractJdbcRowConverter;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.utils.JdbcFieldTypeUtils;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

public class SqlserverJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.SQLSERVER;
    }

    @Override
    protected LocalTime readTime(ResultSet rs, int resultSetIndex) throws SQLException {
        Timestamp sqlTime = JdbcFieldTypeUtils.getTimestamp(rs, resultSetIndex);
        return Optional.ofNullable(sqlTime)
                .map(e -> e.toLocalDateTime().toLocalTime())
                .orElse(null);
    }

    public PreparedStatement toExternal(
            AresRowType rowType, AresRow row, PreparedStatement statement)
            throws SQLException {
        for (int fieldIndex = 0; fieldIndex < rowType.getTotalFields(); fieldIndex++) {
            AresDataType<?> aresDataType = rowType.getFieldType(fieldIndex);
            int statementIndex = fieldIndex + 1;
            Object fieldValue = row.getField(fieldIndex);
            if (fieldValue == null && aresDataType.getSqlType() != SqlType.BYTES) {
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
                    statement.setDate(statementIndex, java.sql.Date.valueOf(localDate));
                    break;
                case TIME:
                    LocalTime localTime = (LocalTime) row.getField(fieldIndex);
                    statement.setTime(statementIndex, java.sql.Time.valueOf(localTime));
                    break;
                case TIMESTAMP:
                    LocalDateTime localDateTime = (LocalDateTime) row.getField(fieldIndex);
                    statement.setTimestamp(
                            statementIndex, Timestamp.valueOf(localDateTime));
                    break;
                case BYTES:
                    if (row.getField(fieldIndex) == null) {
                        statement.setBytes(statementIndex, new byte[0]);
                        break;
                    }
                    statement.setBytes(statementIndex, (byte[]) row.getField(fieldIndex));
                    break;
                case NULL:
                    statement.setNull(statementIndex, java.sql.Types.NULL);
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new JdbcConnectorException(
                            "Unexpected value: " + aresDataType);
            }
        }
        return statement;
    }
}
