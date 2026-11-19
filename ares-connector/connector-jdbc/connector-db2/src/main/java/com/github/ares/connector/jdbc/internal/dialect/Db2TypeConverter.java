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
public class Db2TypeConverter implements TypeConverter<BasicTypeDefine> {

    public static final String DB2_SMALLINT = "SMALLINT";
    public static final String DB2_INTEGER = "INTEGER";
    public static final String DB2_INT = "INT";
    public static final String DB2_BIGINT = "BIGINT";
    public static final String DB2_REAL = "REAL";
    public static final String DB2_DOUBLE = "DOUBLE";
    public static final String DB2_FLOAT = "FLOAT";
    public static final String DB2_DECFLOAT = "DECFLOAT";
    public static final String DB2_DECIMAL = "DECIMAL";
    public static final String DB2_NUMERIC = "NUMERIC";
    public static final String DB2_DEC = "DEC";
    public static final String DB2_BOOLEAN = "BOOLEAN";
    public static final String DB2_CHAR = "CHAR";
    public static final String DB2_CHARACTER = "CHARACTER";
    public static final String DB2_VARCHAR = "VARCHAR";
    public static final String DB2_CLOB = "CLOB";
    public static final String DB2_GRAPHIC = "GRAPHIC";
    public static final String DB2_VARGRAPHIC = "VARGRAPHIC";
    public static final String DB2_DBCLOB = "DBCLOB";
    public static final String DB2_BLOB = "BLOB";
    public static final String DB2_BINARY = "BINARY";
    public static final String DB2_VARBINARY = "VARBINARY";
    public static final String DB2_DATE = "DATE";
    public static final String DB2_TIME = "TIME";
    public static final String DB2_TIMESTAMP = "TIMESTAMP";
    public static final String DB2_XML = "XML";

    public static final int DEFAULT_PRECISION = 31;
    public static final int MAX_PRECISION = 31;
    public static final int DEFAULT_SCALE = 0;
    public static final int MAX_SCALE = MAX_PRECISION;
    public static final long MAX_VARCHAR = 32672L;
    public static final long MAX_VARBINARY = 32672L;

    public static final Db2TypeConverter INSTANCE = new Db2TypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.DB_2;
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
        String db2Type = normalize(typeDefine.getDataType());
        switch (db2Type) {
            case DB2_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case DB2_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case DB2_INTEGER:
            case DB2_INT:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case DB2_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case DB2_REAL:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case DB2_DOUBLE:
            case DB2_FLOAT:
            case DB2_DECFLOAT:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case DB2_DECIMAL:
            case DB2_NUMERIC:
            case DB2_DEC:
                DecimalType decimalType = decimalType(typeDefine);
                builder.dataType(decimalType);
                builder.columnLength((long) decimalType.getPrecision());
                builder.scale(decimalType.getScale());
                break;
            case DB2_CHAR:
            case DB2_CHARACTER:
            case DB2_VARCHAR:
            case DB2_GRAPHIC:
            case DB2_VARGRAPHIC:
            case DB2_CLOB:
            case DB2_DBCLOB:
            case DB2_XML:
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case DB2_BINARY:
            case DB2_VARBINARY:
            case DB2_BLOB:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                builder.columnLength(typeDefine.getLength());
                break;
            case DB2_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case DB2_TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case DB2_TIMESTAMP:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            default:
                throw CommonError.convertToAresTypeError(
                        DatabaseIdentifier.DB_2, typeDefine.getDataType(), typeDefine.getName());
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
                builder.columnType(DB2_BOOLEAN).dataType(DB2_BOOLEAN);
                break;
            case TINYINT:
            case SMALLINT:
                builder.columnType(DB2_SMALLINT).dataType(DB2_SMALLINT);
                break;
            case INT:
                builder.columnType(DB2_INTEGER).dataType(DB2_INTEGER);
                break;
            case BIGINT:
                builder.columnType(DB2_BIGINT).dataType(DB2_BIGINT);
                break;
            case FLOAT:
                builder.columnType(DB2_REAL).dataType(DB2_REAL);
                break;
            case DOUBLE:
                builder.columnType(DB2_DOUBLE).dataType(DB2_DOUBLE);
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
                builder.columnType(String.format("%s(%s,%s)", DB2_DECIMAL, precision, scale));
                builder.dataType(DB2_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case STRING:
                builder.length(column.getColumnLength());
                if (column.getColumnLength() == null
                        || column.getColumnLength() <= 0
                        || column.getColumnLength() > MAX_VARCHAR) {
                    builder.columnType(DB2_CLOB).dataType(DB2_CLOB);
                } else {
                    builder.columnType(
                            String.format("%s(%s)", DB2_VARCHAR, column.getColumnLength()));
                    builder.dataType(DB2_VARCHAR);
                }
                break;
            case BYTES:
                builder.length(column.getColumnLength());
                if (column.getColumnLength() == null
                        || column.getColumnLength() <= 0
                        || column.getColumnLength() > MAX_VARBINARY) {
                    builder.columnType(DB2_BLOB).dataType(DB2_BLOB);
                } else {
                    builder.columnType(
                            String.format("%s(%s)", DB2_VARBINARY, column.getColumnLength()));
                    builder.dataType(DB2_VARBINARY);
                }
                break;
            case DATE:
                builder.columnType(DB2_DATE).dataType(DB2_DATE);
                break;
            case TIME:
                builder.columnType(DB2_TIME).dataType(DB2_TIME);
                break;
            case TIMESTAMP:
                builder.columnType(DB2_TIMESTAMP).dataType(DB2_TIMESTAMP);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.DB_2,
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
