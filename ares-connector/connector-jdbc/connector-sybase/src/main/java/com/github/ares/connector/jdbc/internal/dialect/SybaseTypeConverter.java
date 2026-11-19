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
public class SybaseTypeConverter implements TypeConverter<BasicTypeDefine> {

    public static final String SYB_BIT = "BIT";
    public static final String SYB_TINYINT = "TINYINT";
    public static final String SYB_SMALLINT = "SMALLINT";
    public static final String SYB_INT = "INT";
    public static final String SYB_INTEGER = "INTEGER";
    public static final String SYB_BIGINT = "BIGINT";
    public static final String SYB_FLOAT = "FLOAT";
    public static final String SYB_REAL = "REAL";
    public static final String SYB_DECIMAL = "DECIMAL";
    public static final String SYB_NUMERIC = "NUMERIC";
    public static final String SYB_MONEY = "MONEY";
    public static final String SYB_SMALLMONEY = "SMALLMONEY";
    public static final String SYB_CHAR = "CHAR";
    public static final String SYB_VARCHAR = "VARCHAR";
    public static final String SYB_UNICHAR = "UNICHAR";
    public static final String SYB_UNIVARCHAR = "UNIVARCHAR";
    public static final String SYB_NCHAR = "NCHAR";
    public static final String SYB_NVARCHAR = "NVARCHAR";
    public static final String SYB_TEXT = "TEXT";
    public static final String SYB_UNITEXT = "UNITEXT";
    public static final String SYB_BINARY = "BINARY";
    public static final String SYB_VARBINARY = "VARBINARY";
    public static final String SYB_IMAGE = "IMAGE";
    public static final String SYB_DATE = "DATE";
    public static final String SYB_TIME = "TIME";
    public static final String SYB_DATETIME = "DATETIME";
    public static final String SYB_SMALLDATETIME = "SMALLDATETIME";
    public static final String SYB_BIGDATETIME = "BIGDATETIME";
    public static final String SYB_BIGTIME = "BIGTIME";

    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_SCALE = 0;
    public static final int MAX_SCALE = MAX_PRECISION - 1;
    public static final long MAX_CHAR_LENGTH = 16384L;

    public static final SybaseTypeConverter INSTANCE = new SybaseTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.SYBASE;
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
        String sybType = normalize(typeDefine.getDataType());
        switch (sybType) {
            case SYB_BIT:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case SYB_TINYINT:
            case SYB_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case SYB_INT:
            case SYB_INTEGER:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case SYB_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case SYB_REAL:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case SYB_FLOAT:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case SYB_MONEY:
                builder.dataType(new DecimalType(19, 4));
                builder.columnLength(19L);
                builder.scale(4);
                break;
            case SYB_SMALLMONEY:
                builder.dataType(new DecimalType(10, 4));
                builder.columnLength(10L);
                builder.scale(4);
                break;
            case SYB_DECIMAL:
            case SYB_NUMERIC:
                DecimalType decimalType = decimalType(typeDefine);
                builder.dataType(decimalType);
                builder.columnLength((long) decimalType.getPrecision());
                builder.scale(decimalType.getScale());
                break;
            case SYB_CHAR:
            case SYB_VARCHAR:
            case SYB_NCHAR:
            case SYB_NVARCHAR:
            case SYB_UNICHAR:
            case SYB_UNIVARCHAR:
            case SYB_TEXT:
            case SYB_UNITEXT:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case SYB_BINARY:
            case SYB_VARBINARY:
            case SYB_IMAGE:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case SYB_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case SYB_TIME:
            case SYB_BIGTIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case SYB_DATETIME:
            case SYB_SMALLDATETIME:
            case SYB_BIGDATETIME:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            default:
                throw CommonError.convertToAresTypeError(
                        DatabaseIdentifier.SYBASE, typeDefine.getDataType(), typeDefine.getName());
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
                builder.columnType(SYB_BIT).dataType(SYB_BIT);
                break;
            case TINYINT:
                builder.columnType(SYB_TINYINT).dataType(SYB_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(SYB_SMALLINT).dataType(SYB_SMALLINT);
                break;
            case INT:
                builder.columnType(SYB_INT).dataType(SYB_INT);
                break;
            case BIGINT:
                builder.columnType(SYB_BIGINT).dataType(SYB_BIGINT);
                break;
            case FLOAT:
                builder.columnType(SYB_REAL).dataType(SYB_REAL);
                break;
            case DOUBLE:
                builder.columnType(SYB_FLOAT).dataType(SYB_FLOAT);
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
                builder.columnType(String.format("%s(%s,%s)", SYB_DECIMAL, precision, scale));
                builder.dataType(SYB_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case STRING:
                builder.length(column.getColumnLength());
                if (column.getColumnLength() == null
                        || column.getColumnLength() <= 0
                        || column.getColumnLength() > MAX_CHAR_LENGTH) {
                    builder.columnType(SYB_TEXT).dataType(SYB_TEXT);
                } else {
                    builder.columnType(
                            String.format("%s(%s)", SYB_VARCHAR, column.getColumnLength()));
                    builder.dataType(SYB_VARCHAR);
                }
                break;
            case BYTES:
                builder.length(column.getColumnLength());
                if (column.getColumnLength() == null
                        || column.getColumnLength() <= 0
                        || column.getColumnLength() > MAX_CHAR_LENGTH) {
                    builder.columnType(SYB_IMAGE).dataType(SYB_IMAGE);
                } else {
                    builder.columnType(
                            String.format("%s(%s)", SYB_VARBINARY, column.getColumnLength()));
                    builder.dataType(SYB_VARBINARY);
                }
                break;
            case DATE:
                builder.columnType(SYB_DATE).dataType(SYB_DATE);
                break;
            case TIME:
                builder.columnType(SYB_TIME).dataType(SYB_TIME);
                break;
            case TIMESTAMP:
                builder.columnType(SYB_DATETIME).dataType(SYB_DATETIME);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.SYBASE,
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
