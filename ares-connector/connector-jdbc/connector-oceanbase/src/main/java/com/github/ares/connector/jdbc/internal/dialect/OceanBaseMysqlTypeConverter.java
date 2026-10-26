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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** OceanBase MySQL-mode type mapping. */
@AutoService(TypeConverter.class)
public class OceanBaseMysqlTypeConverter implements TypeConverter<BasicTypeDefine> {
    private static final Logger log = LoggerFactory.getLogger(OceanBaseMysqlTypeConverter.class);

    public static final String OB_BIT = "BIT";
    public static final String OB_TINYINT = "TINYINT";
    public static final String OB_SMALLINT = "SMALLINT";
    public static final String OB_MEDIUMINT = "MEDIUMINT";
    public static final String OB_INT = "INT";
    public static final String OB_INTEGER = "INTEGER";
    public static final String OB_BIGINT = "BIGINT";
    public static final String OB_DECIMAL = "DECIMAL";
    public static final String OB_FLOAT = "FLOAT";
    public static final String OB_DOUBLE = "DOUBLE";
    public static final String OB_CHAR = "CHAR";
    public static final String OB_VARCHAR = "VARCHAR";
    public static final String OB_TINYTEXT = "TINYTEXT";
    public static final String OB_TEXT = "TEXT";
    public static final String OB_MEDIUMTEXT = "MEDIUMTEXT";
    public static final String OB_LONGTEXT = "LONGTEXT";
    public static final String OB_JSON = "JSON";
    public static final String OB_ENUM = "ENUM";
    public static final String OB_SET = "SET";
    public static final String OB_DATE = "DATE";
    public static final String OB_TIME = "TIME";
    public static final String OB_DATETIME = "DATETIME";
    public static final String OB_TIMESTAMP = "TIMESTAMP";
    public static final String OB_YEAR = "YEAR";
    public static final String OB_BINARY = "BINARY";
    public static final String OB_VARBINARY = "VARBINARY";
    public static final String OB_TINYBLOB = "TINYBLOB";
    public static final String OB_BLOB = "BLOB";
    public static final String OB_MEDIUMBLOB = "MEDIUMBLOB";
    public static final String OB_LONGBLOB = "LONGBLOB";
    public static final String OB_GEOMETRY = "GEOMETRY";

    public static final int MAX_PRECISION = 65;
    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_SCALE = 30;
    public static final int DEFAULT_SCALE = 18;
    public static final long MAX_VARCHAR_LENGTH = 65535;
    public static final long MAX_VARBINARY_LENGTH = 65535;

    public static final OceanBaseMysqlTypeConverter INSTANCE = new OceanBaseMysqlTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.OCEANBASE;
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

        String oceanbaseType = normalize(typeDefine.getDataType());
        boolean unsigned = oceanbaseType.endsWith(" UNSIGNED");
        if (unsigned) {
            oceanbaseType =
                    oceanbaseType.substring(0, oceanbaseType.length() - " UNSIGNED".length());
        }
        switch (oceanbaseType) {
            case OB_BIT:
                if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 1) {
                    builder.dataType(PrimitiveByteArrayType.INSTANCE);
                } else {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                }
                break;
            case OB_TINYINT:
            case OB_SMALLINT:
            case OB_MEDIUMINT:
            case OB_INT:
            case OB_INTEGER:
            case OB_YEAR:
                builder.dataType(unsigned ? BasicType.LONG_TYPE : BasicType.INT_TYPE);
                break;
            case OB_BIGINT:
                if (unsigned) {
                    builder.dataType(new DecimalType(20, 0));
                } else {
                    builder.dataType(BasicType.LONG_TYPE);
                }
                break;
            case OB_DECIMAL:
                convertDecimal(builder, typeDefine, unsigned);
                break;
            case OB_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case OB_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case OB_CHAR:
            case OB_VARCHAR:
            case OB_TINYTEXT:
            case OB_TEXT:
            case OB_MEDIUMTEXT:
            case OB_LONGTEXT:
            case OB_JSON:
            case OB_ENUM:
            case OB_SET:
            case OB_GEOMETRY:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case OB_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case OB_TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                break;
            case OB_DATETIME:
            case OB_TIMESTAMP:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case OB_BINARY:
            case OB_VARBINARY:
            case OB_TINYBLOB:
            case OB_BLOB:
            case OB_MEDIUMBLOB:
            case OB_LONGBLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            default:
                throw CommonError.convertToAresTypeError(
                        DatabaseIdentifier.OCEANBASE,
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
                builder.columnType(OB_TINYINT + "(1)");
                builder.dataType(OB_TINYINT);
                break;
            case TINYINT:
                builder.columnType(OB_TINYINT);
                builder.dataType(OB_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(OB_SMALLINT);
                builder.dataType(OB_SMALLINT);
                break;
            case INT:
                builder.columnType(OB_INT);
                builder.dataType(OB_INT);
                break;
            case BIGINT:
                builder.columnType(OB_BIGINT);
                builder.dataType(OB_BIGINT);
                break;
            case FLOAT:
                builder.columnType(OB_FLOAT);
                builder.dataType(OB_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(OB_DOUBLE);
                builder.dataType(OB_DOUBLE);
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
                            "The decimal column {} type decimal({},{}) exceeds the maximum precision of {}",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_PRECISION);
                }
                if (scale < 0) {
                    scale = 0;
                } else if (scale > MAX_SCALE) {
                    scale = MAX_SCALE;
                }
                builder.columnType(String.format("%s(%s,%s)", OB_DECIMAL, precision, scale));
                builder.dataType(OB_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case BYTES:
                if (column.getColumnLength() == null
                        || column.getColumnLength() <= 0
                        || column.getColumnLength() > MAX_VARBINARY_LENGTH) {
                    builder.columnType(OB_LONGBLOB);
                    builder.dataType(OB_LONGBLOB);
                } else {
                    builder.columnType(
                            String.format("%s(%s)", OB_VARBINARY, column.getColumnLength()));
                    builder.dataType(OB_VARBINARY);
                }
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(OB_LONGTEXT);
                    builder.dataType(OB_LONGTEXT);
                } else if (column.getColumnLength() <= MAX_VARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", OB_VARCHAR, column.getColumnLength()));
                    builder.dataType(OB_VARCHAR);
                } else {
                    builder.columnType(OB_LONGTEXT);
                    builder.dataType(OB_LONGTEXT);
                }
                break;
            case DATE:
                builder.columnType(OB_DATE);
                builder.dataType(OB_DATE);
                break;
            case TIME:
                builder.columnType(OB_TIME);
                builder.dataType(OB_TIME);
                break;
            case TIMESTAMP:
                builder.columnType(OB_DATETIME);
                builder.dataType(OB_DATETIME);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.OCEANBASE,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }

    private void convertDecimal(
            PhysicalColumn.PhysicalColumnBuilder builder,
            BasicTypeDefine typeDefine,
            boolean unsigned) {
        long precision =
                typeDefine.getPrecision() == null || typeDefine.getPrecision() <= 0
                        ? DEFAULT_PRECISION
                        : typeDefine.getPrecision();
        int scale = typeDefine.getScale() == null ? DEFAULT_SCALE : typeDefine.getScale();
        if (unsigned) {
            precision = precision + 1;
        }
        if (precision > MAX_PRECISION) {
            precision = MAX_PRECISION;
        }
        if (scale < 0) {
            scale = 0;
        } else if (scale > MAX_SCALE) {
            scale = MAX_SCALE;
        }
        if (precision <= scale) {
            precision = Math.min(MAX_PRECISION, scale + 1L);
        }
        builder.dataType(new DecimalType((int) precision, scale));
        builder.columnLength(precision);
        builder.scale(scale);
    }

    static String normalize(String dataType) {
        return dataType.toUpperCase().replaceAll("\\([^)]*\\)", "").replaceAll("\\s+", " ").trim();
    }
}
