package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.PhysicalColumn;
import com.github.ares.api.table.catalog.PrimitiveByteArrayType;
import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.api.table.connector.TypeConverter;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.common.exceptions.CommonError;
import com.google.auto.service.AutoService;
import java.util.Locale;

@AutoService(TypeConverter.class)
public class VerticaTypeConverter implements TypeConverter<BasicTypeDefine> {

    public static final String VT_BOOLEAN = "BOOLEAN";
    public static final String VT_TINYINT = "TINYINT";
    public static final String VT_SMALLINT = "SMALLINT";
    public static final String VT_INTEGER = "INTEGER";
    public static final String VT_INT = "INT";
    public static final String VT_BIGINT = "BIGINT";
    public static final String VT_REAL = "REAL";
    public static final String VT_FLOAT = "FLOAT";
    public static final String VT_FLOAT4 = "FLOAT4";
    public static final String VT_FLOAT8 = "FLOAT8";
    public static final String VT_DOUBLE = "DOUBLE PRECISION";
    public static final String VT_NUMERIC = "NUMERIC";
    public static final String VT_DECIMAL = "DECIMAL";
    public static final String VT_NUMBER = "NUMBER";
    public static final String VT_CHAR = "CHAR";
    public static final String VT_CHARACTER = "CHARACTER";
    public static final String VT_VARCHAR = "VARCHAR";
    public static final String VT_LONG_VARCHAR = "LONG VARCHAR";
    public static final String VT_BINARY = "BINARY";
    public static final String VT_VARBINARY = "VARBINARY";
    public static final String VT_LONG_VARBINARY = "LONG VARBINARY";
    public static final String VT_BYTEA = "BYTEA";
    public static final String VT_DATE = "DATE";
    public static final String VT_TIME = "TIME";
    public static final String VT_TIMETZ = "TIMETZ";
    public static final String VT_TIMESTAMP = "TIMESTAMP";
    public static final String VT_TIMESTAMPTZ = "TIMESTAMPTZ";
    public static final String VT_INTERVAL = "INTERVAL";
    public static final String VT_UUID = "UUID";

    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_PRECISION = 1024;
    public static final int DEFAULT_SCALE = 0;
    public static final int MAX_SCALE = 1024;
    public static final long MAX_VARCHAR = 65000L;
    public static final long MAX_VARBINARY = 65000L;

    public static final VerticaTypeConverter INSTANCE = new VerticaTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.VERTICA;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        String verticaType = normalize(typeDefine.getDataType());
        switch (verticaType) {
            case VT_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case VT_TINYINT:
            case VT_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case VT_INTEGER:
            case VT_INT:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case VT_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case VT_REAL:
            case VT_FLOAT4:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case VT_FLOAT:
            case VT_FLOAT8:
            case VT_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case VT_NUMERIC:
            case VT_DECIMAL:
            case VT_NUMBER:
                DecimalType decimalType = decimalType(typeDefine);
                builder.dataType(decimalType);
                builder.columnLength((long) decimalType.getPrecision());
                builder.scale(decimalType.getScale());
                break;
            case VT_CHAR:
            case VT_CHARACTER:
            case VT_VARCHAR:
            case VT_LONG_VARCHAR:
            case VT_INTERVAL:
            case VT_UUID:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case VT_BINARY:
            case VT_VARBINARY:
            case VT_LONG_VARBINARY:
            case VT_BYTEA:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case VT_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case VT_TIME:
            case VT_TIMETZ:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case VT_TIMESTAMP:
            case VT_TIMESTAMPTZ:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            default:
                throw CommonError.convertToAresTypeError(
                        DatabaseIdentifier.VERTICA, typeDefine.getDataType(), typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());
        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                builder.columnType(VT_BOOLEAN).dataType(VT_BOOLEAN);
                break;
            case TINYINT:
                builder.columnType(VT_TINYINT).dataType(VT_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(VT_SMALLINT).dataType(VT_SMALLINT);
                break;
            case INT:
                builder.columnType(VT_INTEGER).dataType(VT_INTEGER);
                break;
            case BIGINT:
                builder.columnType(VT_BIGINT).dataType(VT_BIGINT);
                break;
            case FLOAT:
                builder.columnType(VT_REAL).dataType(VT_REAL);
                break;
            case DOUBLE:
                builder.columnType(VT_FLOAT).dataType(VT_FLOAT);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                long precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (precision <= 0) {
                    precision = DEFAULT_PRECISION;
                    scale = DEFAULT_SCALE;
                } else if (precision > MAX_PRECISION) {
                    scale = (int) Math.max(0, scale - (precision - MAX_PRECISION));
                    precision = MAX_PRECISION;
                }
                if (scale < 0) {
                    scale = 0;
                } else if (scale > MAX_SCALE) {
                    scale = MAX_SCALE;
                }
                builder.columnType(String.format("%s(%s,%s)", VT_NUMERIC, precision, scale));
                builder.dataType(VT_NUMERIC);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case STRING:
                builder.length(column.getColumnLength());
                if (column.getColumnLength() == null
                        || column.getColumnLength() <= 0
                        || column.getColumnLength() > MAX_VARCHAR) {
                    builder.columnType(VT_LONG_VARCHAR).dataType(VT_LONG_VARCHAR);
                } else {
                    builder.columnType(
                            String.format("%s(%s)", VT_VARCHAR, column.getColumnLength()));
                    builder.dataType(VT_VARCHAR);
                }
                break;
            case BYTES:
                builder.length(column.getColumnLength());
                if (column.getColumnLength() == null
                        || column.getColumnLength() <= 0
                        || column.getColumnLength() > MAX_VARBINARY) {
                    builder.columnType(VT_LONG_VARBINARY).dataType(VT_LONG_VARBINARY);
                } else {
                    builder.columnType(
                            String.format("%s(%s)", VT_VARBINARY, column.getColumnLength()));
                    builder.dataType(VT_VARBINARY);
                }
                break;
            case DATE:
                builder.columnType(VT_DATE).dataType(VT_DATE);
                break;
            case TIME:
                builder.columnType(VT_TIME).dataType(VT_TIME);
                break;
            case TIMESTAMP:
                builder.columnType(VT_TIMESTAMP).dataType(VT_TIMESTAMP);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.VERTICA,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }

    static String normalize(String dataType) {
        if (dataType == null) {
            return "";
        }
        String raw = dataType.toUpperCase(Locale.ROOT).trim();
        int paren = raw.indexOf('(');
        if (paren > 0) {
            raw = raw.substring(0, paren).trim();
        }
        if (raw.startsWith(VT_LONG_VARCHAR)) {
            return VT_LONG_VARCHAR;
        }
        if (raw.startsWith(VT_LONG_VARBINARY)) {
            return VT_LONG_VARBINARY;
        }
        if (raw.startsWith(VT_DOUBLE)) {
            return VT_DOUBLE;
        }
        if (raw.contains(" ")) {
            raw = raw.split("\\s+")[0];
        }
        return raw;
    }

    private static DecimalType decimalType(BasicTypeDefine typeDefine) {
        if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
            int scale = typeDefine.getScale() == null ? 0 : typeDefine.getScale();
            int precision = typeDefine.getPrecision().intValue();
            if (precision > MAX_PRECISION) {
                scale = Math.max(0, scale - (precision - MAX_PRECISION));
                precision = MAX_PRECISION;
            }
            if (scale > MAX_SCALE) {
                scale = MAX_SCALE;
            }
            return new DecimalType(precision, Math.max(scale, 0));
        }
        return new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
    }
}
