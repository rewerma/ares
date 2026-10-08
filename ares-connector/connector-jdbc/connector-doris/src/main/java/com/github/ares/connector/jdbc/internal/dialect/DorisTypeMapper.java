package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.catalog.PrimitiveByteArrayType;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.LocalTimeType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DorisTypeMapper implements JdbcDialectTypeMapper {
    private static final Logger LOG = LoggerFactory.getLogger(DorisTypeMapper.class);

    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_SCALE = 18;

    @Override
    public AresDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnType = metadata.getColumnTypeName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        return map(columnType, precision, scale, metadata.getColumnName(colIndex));
    }

    public AresDataType<?> map(String columnType, int precision, int scale, String columnName) {
        String type = normalize(columnType);
        switch (type) {
            case "NULL":
                return BasicType.VOID_TYPE;
            case "BOOLEAN":
            case "BOOL":
                return BasicType.BOOLEAN_TYPE;
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "INTEGER":
            case "MEDIUMINT":
                return BasicType.INT_TYPE;
            case "BIGINT":
                return BasicType.LONG_TYPE;
            case "LARGEINT":
                return new DecimalType(MAX_PRECISION, 0);
            case "FLOAT":
                return BasicType.FLOAT_TYPE;
            case "DOUBLE":
                return BasicType.DOUBLE_TYPE;
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMALV3":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
            case "DECIMAL256":
                int decimalPrecision = precision <= 0 ? MAX_PRECISION : precision;
                int decimalScale = precision <= 0 ? DEFAULT_SCALE : Math.max(scale, 0);
                if (decimalPrecision > MAX_PRECISION) {
                    LOG.warn(
                            "Column {} type {}({},{}) exceeds precision {}, narrowing to ({},{})",
                            columnName,
                            type,
                            decimalPrecision,
                            decimalScale,
                            MAX_PRECISION,
                            MAX_PRECISION,
                            Math.max(0, decimalScale - (decimalPrecision - MAX_PRECISION)));
                    decimalScale = Math.max(0, decimalScale - (decimalPrecision - MAX_PRECISION));
                    decimalPrecision = MAX_PRECISION;
                }
                if (decimalScale > decimalPrecision) {
                    decimalScale = decimalPrecision;
                }
                return new DecimalType(decimalPrecision, decimalScale);
            case "DATE":
            case "DATEV2":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "TIME":
                return LocalTimeType.LOCAL_TIME_TYPE;
            case "DATETIME":
            case "DATETIMEV2":
            case "TIMESTAMP":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "CHAR":
            case "VARCHAR":
            case "STRING":
            case "TEXT":
            case "JSON":
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "HLL":
            case "BITMAP":
            case "QUANTILE_STATE":
            case "AGG_STATE":
            case "PERCENTILE":
            case "VARIANT":
            case "IPV4":
            case "IPV6":
                return BasicType.STRING_TYPE;
            case "BINARY":
            case "VARBINARY":
                return PrimitiveByteArrayType.INSTANCE;
            default:
                LOG.warn("Doris type {} of column {} is mapped to STRING", type, columnName);
                return BasicType.STRING_TYPE;
        }
    }

    static String normalize(String columnType) {
        if (columnType == null) {
            return "STRING";
        }
        String type = columnType.trim().toUpperCase();
        int paren = type.indexOf('(');
        if (paren > 0) {
            type = type.substring(0, paren).trim();
        }
        if (type.endsWith(" UNSIGNED")) {
            type = type.substring(0, type.length() - " UNSIGNED".length()).trim();
        }
        return type;
    }
}
