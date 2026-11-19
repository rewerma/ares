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
public class Gbase8sTypeConverter implements TypeConverter<BasicTypeDefine> {

    public static final String GB_BOOLEAN = "BOOLEAN";
    public static final String GB_SMALLINT = "SMALLINT";
    public static final String GB_INTEGER = "INTEGER";
    public static final String GB_INT = "INT";
    public static final String GB_INT8 = "INT8";
    public static final String GB_BIGINT = "BIGINT";
    public static final String GB_SERIAL = "SERIAL";
    public static final String GB_SERIAL8 = "SERIAL8";
    public static final String GB_BIGSERIAL = "BIGSERIAL";
    public static final String GB_FLOAT = "FLOAT";
    public static final String GB_SMALLFLOAT = "SMALLFLOAT";
    public static final String GB_DECIMAL = "DECIMAL";
    public static final String GB_NUMERIC = "NUMERIC";
    public static final String GB_MONEY = "MONEY";
    public static final String GB_CHAR = "CHAR";
    public static final String GB_NCHAR = "NCHAR";
    public static final String GB_VARCHAR = "VARCHAR";
    public static final String GB_NVARCHAR = "NVARCHAR";
    public static final String GB_LVARCHAR = "LVARCHAR";
    public static final String GB_TEXT = "TEXT";
    public static final String GB_BYTE = "BYTE";
    public static final String GB_BLOB = "BLOB";
    public static final String GB_CLOB = "CLOB";
    public static final String GB_DATE = "DATE";
    public static final String GB_DATETIME = "DATETIME";
    public static final String GB_INTERVAL = "INTERVAL";

    public static final int DEFAULT_PRECISION = 32;
    public static final int MAX_PRECISION = 32;
    public static final int DEFAULT_SCALE = 16;
    public static final int MAX_SCALE = MAX_PRECISION - 1;
    public static final long MAX_VARCHAR_LENGTH = 255L;

    public static final Gbase8sTypeConverter INSTANCE = new Gbase8sTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.GBASE_8S;
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
        String gbType = normalize(typeDefine.getDataType());
        switch (gbType) {
            case GB_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case GB_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case GB_INTEGER:
            case GB_INT:
            case GB_SERIAL:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case GB_INT8:
            case GB_BIGINT:
            case GB_SERIAL8:
            case GB_BIGSERIAL:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case GB_SMALLFLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case GB_FLOAT:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case GB_MONEY:
                builder.dataType(new DecimalType(16, 2));
                builder.columnLength(16L);
                builder.scale(2);
                break;
            case GB_DECIMAL:
            case GB_NUMERIC:
                DecimalType decimalType = decimalType(typeDefine);
                builder.dataType(decimalType);
                builder.columnLength((long) decimalType.getPrecision());
                builder.scale(decimalType.getScale());
                break;
            case GB_CHAR:
            case GB_NCHAR:
            case GB_VARCHAR:
            case GB_NVARCHAR:
            case GB_LVARCHAR:
            case GB_TEXT:
            case GB_CLOB:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case GB_BYTE:
            case GB_BLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case GB_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case GB_DATETIME:
            case GB_INTERVAL:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            default:
                throw CommonError.convertToAresTypeError(
                        DatabaseIdentifier.GBASE_8S,
                        typeDefine.getDataType(),
                        typeDefine.getName());
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
                builder.columnType(GB_BOOLEAN).dataType(GB_BOOLEAN);
                break;
            case TINYINT:
            case SMALLINT:
                builder.columnType(GB_SMALLINT).dataType(GB_SMALLINT);
                break;
            case INT:
                builder.columnType(GB_INTEGER).dataType(GB_INTEGER);
                break;
            case BIGINT:
                builder.columnType(GB_BIGINT).dataType(GB_BIGINT);
                break;
            case FLOAT:
                builder.columnType(GB_SMALLFLOAT).dataType(GB_SMALLFLOAT);
                break;
            case DOUBLE:
                builder.columnType(GB_FLOAT).dataType(GB_FLOAT);
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
                builder.columnType(String.format("%s(%s,%s)", GB_DECIMAL, precision, scale));
                builder.dataType(GB_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case STRING:
                builder.length(column.getColumnLength());
                if (column.getColumnLength() == null
                        || column.getColumnLength() <= 0
                        || column.getColumnLength() > MAX_VARCHAR_LENGTH) {
                    builder.columnType(GB_TEXT).dataType(GB_TEXT);
                } else {
                    builder.columnType(
                            String.format("%s(%s)", GB_VARCHAR, column.getColumnLength()));
                    builder.dataType(GB_VARCHAR);
                }
                break;
            case BYTES:
                builder.columnType(GB_BYTE).dataType(GB_BYTE);
                builder.length(column.getColumnLength());
                break;
            case DATE:
                builder.columnType(GB_DATE).dataType(GB_DATE);
                break;
            case TIME:
            case TIMESTAMP:
                builder.columnType(GB_DATETIME).dataType(GB_DATETIME);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.GBASE_8S,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }

    private static String normalize(String dataType) {
        if (dataType == null) {
            return "";
        }
        String raw = dataType.toUpperCase(Locale.ROOT).trim();
        int paren = raw.indexOf('(');
        if (paren > 0) {
            raw = raw.substring(0, paren).trim();
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
