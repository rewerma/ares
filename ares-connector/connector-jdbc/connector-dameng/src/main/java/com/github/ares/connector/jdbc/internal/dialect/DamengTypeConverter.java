package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.source.TypeDefineUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dameng type mapping. Numeric and character limits follow DM8. */
@AutoService(TypeConverter.class)
public class DamengTypeConverter implements TypeConverter<BasicTypeDefine> {
    private static final Logger log = LoggerFactory.getLogger(DamengTypeConverter.class);

    public static final String DM_BIT = "BIT";
    public static final String DM_TINYINT = "TINYINT";
    public static final String DM_BYTE = "BYTE";
    public static final String DM_SMALLINT = "SMALLINT";
    public static final String DM_INT = "INT";
    public static final String DM_INTEGER = "INTEGER";
    public static final String DM_PLS_INTEGER = "PLS_INTEGER";
    public static final String DM_BIGINT = "BIGINT";
    public static final String DM_NUMBER = "NUMBER";
    public static final String DM_NUMERIC = "NUMERIC";
    public static final String DM_DECIMAL = "DECIMAL";
    public static final String DM_DEC = "DEC";
    public static final String DM_FLOAT = "FLOAT";
    public static final String DM_REAL = "REAL";
    public static final String DM_DOUBLE = "DOUBLE";
    public static final String DM_DOUBLE_PRECISION = "DOUBLE PRECISION";
    public static final String DM_BINARY_FLOAT = "BINARY_FLOAT";
    public static final String DM_BINARY_DOUBLE = "BINARY_DOUBLE";

    public static final String DM_CHAR = "CHAR";
    public static final String DM_CHARACTER = "CHARACTER";
    public static final String DM_NCHAR = "NCHAR";
    public static final String DM_VARCHAR = "VARCHAR";
    public static final String DM_VARCHAR2 = "VARCHAR2";
    public static final String DM_NVARCHAR = "NVARCHAR";
    public static final String DM_NVARCHAR2 = "NVARCHAR2";
    public static final String DM_TEXT = "TEXT";
    public static final String DM_LONG = "LONG";
    public static final String DM_LONGVARCHAR = "LONGVARCHAR";
    public static final String DM_CLOB = "CLOB";
    public static final String DM_ROWID = "ROWID";

    public static final String DM_DATE = "DATE";
    public static final String DM_TIME = "TIME";
    public static final String DM_TIMESTAMP = "TIMESTAMP";
    public static final String DM_DATETIME = "DATETIME";
    public static final String DM_TIMESTAMP_WITH_TIME_ZONE = "TIMESTAMP WITH TIME ZONE";
    public static final String DM_TIMESTAMP_WITH_LOCAL_TIME_ZONE = "TIMESTAMP WITH LOCAL TIME ZONE";

    public static final String DM_BINARY = "BINARY";
    public static final String DM_VARBINARY = "VARBINARY";
    public static final String DM_BLOB = "BLOB";
    public static final String DM_IMAGE = "IMAGE";
    public static final String DM_LONGVARBINARY = "LONGVARBINARY";
    public static final String DM_BFILE = "BFILE";

    public static final int MAX_PRECISION = 38;
    public static final int DEFAULT_PRECISION = MAX_PRECISION;
    public static final int MAX_SCALE = 38;
    public static final int DEFAULT_SCALE = 18;
    public static final int TIMESTAMP_DEFAULT_SCALE = 6;
    public static final int MAX_TIMESTAMP_SCALE = 6;
    public static final long MAX_CHAR_LENGTH = 32767;
    public static final long MAX_BINARY_LENGTH = 32767;
    public static final long MAX_ROWID_LENGTH = 18;
    public static final long BYTES_2GB = (long) Math.pow(2, 31);

    public static final DamengTypeConverter INSTANCE = new DamengTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.DAMENG;
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

        String damengType = normalize(typeDefine.getDataType());
        switch (damengType) {
            case DM_BIT:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case DM_TINYINT:
            case DM_BYTE:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case DM_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case DM_INT:
            case DM_INTEGER:
            case DM_PLS_INTEGER:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case DM_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case DM_NUMBER:
            case DM_NUMERIC:
            case DM_DECIMAL:
            case DM_DEC:
                convertDecimal(builder, typeDefine);
                break;
            case DM_REAL:
            case DM_BINARY_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case DM_FLOAT:
            case DM_DOUBLE:
            case DM_DOUBLE_PRECISION:
            case DM_BINARY_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case DM_CHAR:
            case DM_CHARACTER:
            case DM_VARCHAR:
            case DM_VARCHAR2:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case DM_NCHAR:
            case DM_NVARCHAR:
            case DM_NVARCHAR2:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(
                        TypeDefineUtils.doubleByteTo4ByteLength(typeDefine.getLength()));
                break;
            case DM_ROWID:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(MAX_ROWID_LENGTH);
                break;
            case DM_TEXT:
            case DM_LONG:
            case DM_LONGVARCHAR:
            case DM_CLOB:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(BYTES_2GB - 1);
                break;
            case DM_BINARY:
            case DM_VARBINARY:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                if (typeDefine.getLength() == null || typeDefine.getLength() == 0) {
                    builder.columnLength(MAX_BINARY_LENGTH);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case DM_BLOB:
            case DM_IMAGE:
            case DM_LONGVARBINARY:
            case DM_BFILE:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(BYTES_2GB - 1);
                break;
            case DM_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case DM_TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            case DM_TIMESTAMP:
            case DM_DATETIME:
            case DM_TIMESTAMP_WITH_TIME_ZONE:
            case DM_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                if (typeDefine.getScale() == null) {
                    builder.scale(TIMESTAMP_DEFAULT_SCALE);
                } else {
                    builder.scale(typeDefine.getScale());
                }
                break;
            default:
                throw CommonError.convertToAresTypeError(
                        DatabaseIdentifier.DAMENG, damengType, typeDefine.getName());
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
                builder.columnType(DM_BIT);
                builder.dataType(DM_BIT);
                break;
            case TINYINT:
                builder.columnType(DM_TINYINT);
                builder.dataType(DM_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(DM_SMALLINT);
                builder.dataType(DM_SMALLINT);
                break;
            case INT:
                builder.columnType(DM_INT);
                builder.dataType(DM_INT);
                break;
            case BIGINT:
                builder.columnType(DM_BIGINT);
                builder.dataType(DM_BIGINT);
                break;
            case FLOAT:
                builder.columnType(DM_REAL);
                builder.dataType(DM_REAL);
                break;
            case DOUBLE:
                builder.columnType(DM_DOUBLE);
                builder.dataType(DM_DOUBLE);
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
                    log.warn(
                            "The decimal column {} type decimal({},{}) exceeds the maximum precision of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_PRECISION,
                            precision,
                            scale);
                }
                if (scale < 0) {
                    scale = 0;
                } else if (scale > MAX_SCALE) {
                    scale = MAX_SCALE;
                }
                builder.columnType(String.format("%s(%s,%s)", DM_DECIMAL, precision, scale));
                builder.dataType(DM_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                if (column.getColumnLength() == null
                        || column.getColumnLength() <= 0
                        || column.getColumnLength() > MAX_BINARY_LENGTH) {
                    builder.columnType(DM_BLOB);
                    builder.dataType(DM_BLOB);
                } else {
                    builder.columnType(
                            String.format("%s(%s)", DM_VARBINARY, column.getColumnLength()));
                    builder.dataType(DM_VARBINARY);
                }
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(String.format("%s(%s)", DM_VARCHAR, MAX_CHAR_LENGTH));
                    builder.dataType(DM_VARCHAR);
                } else if (column.getColumnLength() <= MAX_CHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", DM_VARCHAR, column.getColumnLength()));
                    builder.dataType(DM_VARCHAR);
                } else {
                    builder.columnType(DM_CLOB);
                    builder.dataType(DM_CLOB);
                }
                break;
            case DATE:
                builder.columnType(DM_DATE);
                builder.dataType(DM_DATE);
                break;
            case TIME:
                builder.columnType(DM_TIME);
                builder.dataType(DM_TIME);
                break;
            case TIMESTAMP:
                if (column.getScale() == null || column.getScale() <= 0) {
                    builder.columnType(DM_TIMESTAMP);
                    builder.dataType(DM_TIMESTAMP);
                } else {
                    int timestampScale = Math.min(column.getScale(), MAX_TIMESTAMP_SCALE);
                    builder.columnType(String.format("TIMESTAMP(%s)", timestampScale));
                    builder.dataType(DM_TIMESTAMP);
                    builder.scale(timestampScale);
                }
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.DAMENG,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }

    private void convertDecimal(
            PhysicalColumn.PhysicalColumnBuilder builder, BasicTypeDefine typeDefine) {
        Long precision = typeDefine.getPrecision();
        if (precision == null || precision == 0 || precision > DEFAULT_PRECISION) {
            precision = Long.valueOf(DEFAULT_PRECISION);
        }
        Integer scale = typeDefine.getScale();
        if (scale == null) {
            scale = MAX_SCALE;
        }

        if (scale == 0) {
            if (precision == 1) {
                builder.dataType(BasicType.BOOLEAN_TYPE);
            } else if (precision <= 9) {
                builder.dataType(BasicType.INT_TYPE);
            } else if (precision <= 18) {
                builder.dataType(BasicType.LONG_TYPE);
            } else {
                builder.dataType(new DecimalType(precision.intValue(), 0));
                builder.columnLength(precision);
            }
        } else if (scale > 0 && scale <= DEFAULT_SCALE) {
            builder.dataType(new DecimalType(precision.intValue(), scale));
            builder.columnLength(precision);
            builder.scale(scale);
        } else {
            int normalizedScale = scale < 0 ? 0 : DEFAULT_SCALE;
            builder.dataType(new DecimalType(precision.intValue(), normalizedScale));
            builder.columnLength(precision);
            builder.scale(normalizedScale);
        }
    }

    private static String normalize(String dataType) {
        String type = dataType.toUpperCase().trim();
        int paren = type.indexOf('(');
        if (paren > 0) {
            type = type.substring(0, paren).trim();
        }
        return type;
    }
}
