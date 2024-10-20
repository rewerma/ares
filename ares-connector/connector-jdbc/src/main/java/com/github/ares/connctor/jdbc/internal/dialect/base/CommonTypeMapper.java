package com.github.ares.connctor.jdbc.internal.dialect.base;

import com.github.ares.api.table.catalog.PrimitiveByteArrayType;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class CommonTypeMapper implements JdbcDialectTypeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialect.class);

    // ============================data types=====================

    private static final String COMMON_UNKNOWN = "UNKNOWN";
    private static final String COMMON_BIT = "BIT";

    // -------------------------number----------------------------
    private static final String COMMON_TINYINT = "TINYINT";
    private static final String COMMON_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String COMMON_SMALLINT = "SMALLINT";
    private static final String COMMON_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String COMMON_MEDIUMINT = "MEDIUMINT";
    private static final String COMMON_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String COMMON_INT = "INT";
    private static final String COMMON_INT_UNSIGNED = "INT UNSIGNED";
    private static final String COMMON_INTEGER = "INTEGER";
    private static final String COMMON_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String COMMON_BIGINT = "BIGINT";
    private static final String COMMON_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String COMMON_DECIMAL = "DECIMAL";
    private static final String COMMON_NUMBER = "NUMBER";
    private static final String COMMON_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String COMMON_FLOAT = "FLOAT";
    private static final String COMMON_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String COMMON_DOUBLE = "DOUBLE";
    private static final String COMMON_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    private static final String COMMON_CHAR = "CHAR";
    private static final String COMMON_VARCHAR = "VARCHAR";
    private static final String COMMON_VARCHAR2 = "VARCHAR2";
    private static final String COMMON_TINYTEXT = "TINYTEXT";
    private static final String COMMON_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String COMMON_TEXT = "TEXT";
    private static final String COMMON_LONGTEXT = "LONGTEXT";
    private static final String COMMON_JSON = "JSON";

    // ------------------------------time-------------------------
    private static final String COMMON_DATE = "DATE";
    private static final String COMMON_DATETIME = "DATETIME";
    private static final String COMMON_TIME = "TIME";
    private static final String COMMON_TIMESTAMP = "TIMESTAMP";
    private static final String COMMON_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    private static final String COMMON_TINYBLOB = "TINYBLOB";
    private static final String COMMON_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String COMMON_BLOB = "BLOB";
    private static final String COMMON_LONGBLOB = "LONGBLOB";
    private static final String COMMON_BINARY = "BINARY";
    private static final String COMMON_VARBINARY = "VARBINARY";
    private static final String COMMON_GEOMETRY = "GEOMETRY";

    @Override
    public AresDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex).toUpperCase();
        String columnName = metadata.getColumnName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (mysqlType) {
            case COMMON_BIT:
                if (precision == 1) {
                    return BasicType.BOOLEAN_TYPE;
                } else {
                    return PrimitiveByteArrayType.INSTANCE;
                }
            case COMMON_TINYINT:
            case COMMON_TINYINT_UNSIGNED:
            case COMMON_SMALLINT:
            case COMMON_SMALLINT_UNSIGNED:
            case COMMON_MEDIUMINT:
            case COMMON_MEDIUMINT_UNSIGNED:
            case COMMON_INT:
            case COMMON_INTEGER:
            case COMMON_YEAR:
                return BasicType.INT_TYPE;
            case COMMON_INT_UNSIGNED:
            case COMMON_INTEGER_UNSIGNED:
            case COMMON_BIGINT:
                return BasicType.LONG_TYPE;
            case COMMON_BIGINT_UNSIGNED:
                return new DecimalType(20, 0);
            case COMMON_NUMBER:
            case COMMON_DECIMAL:
                if (precision > 38) {
                    LOG.warn("{} will probably cause value overflow.", COMMON_DECIMAL);
                    return new DecimalType(38, 18);
                }
                return new DecimalType(precision, scale);
            case COMMON_DECIMAL_UNSIGNED:
                return new DecimalType(precision + 1, scale);
            case COMMON_FLOAT:
                return BasicType.FLOAT_TYPE;
            case COMMON_FLOAT_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", COMMON_FLOAT_UNSIGNED);
                return BasicType.FLOAT_TYPE;
            case COMMON_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case COMMON_DOUBLE_UNSIGNED:
                LOG.warn("{} will probably cause value overflow.", COMMON_DOUBLE_UNSIGNED);
                return BasicType.DOUBLE_TYPE;
            case COMMON_CHAR:
            case COMMON_TINYTEXT:
            case COMMON_MEDIUMTEXT:
            case COMMON_TEXT:
            case COMMON_VARCHAR:
            case COMMON_VARCHAR2:
            case COMMON_JSON:
                return BasicType.STRING_TYPE;
            case COMMON_LONGTEXT:
                LOG.warn(
                        "Type '{}' has a maximum precision of 536870911 in MySQL. "
                                + "Due to limitations in the ares type system, "
                                + "the precision will be set to 2147483647.",
                        COMMON_LONGTEXT);
                return BasicType.STRING_TYPE;
            case COMMON_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case COMMON_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case COMMON_DATETIME:
            case COMMON_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;

            case COMMON_TINYBLOB:
            case COMMON_MEDIUMBLOB:
            case COMMON_BLOB:
            case COMMON_LONGBLOB:
            case COMMON_VARBINARY:
            case COMMON_BINARY:
                return PrimitiveByteArrayType.INSTANCE;

            // Doesn't support yet
            case COMMON_GEOMETRY:
            case COMMON_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new AresException(DatabaseIdentifier.MYSQL + " unsupported convert type " + mysqlType + " of " + jdbcColumnName);
        }
    }
}
