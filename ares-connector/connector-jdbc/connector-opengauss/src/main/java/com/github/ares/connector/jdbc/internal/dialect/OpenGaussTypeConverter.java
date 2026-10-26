package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.source.TypeDefineUtils;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.PhysicalColumn;
import com.github.ares.api.table.catalog.PrimitiveByteArrayType;
import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.api.table.connector.TypeConverter;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.common.exceptions.CommonError;
import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** OpenGauss type mapping. JDBC type names follow PostgreSQL, plus tinyint, blob and clob. */
@AutoService(TypeConverter.class)
public class OpenGaussTypeConverter implements TypeConverter<BasicTypeDefine> {
    private static final Logger log = LoggerFactory.getLogger(OpenGaussTypeConverter.class);

    public static final String OG_BOOLEAN = "bool";
    public static final String OG_BOOLEAN_ARRAY = "_bool";
    public static final String OG_BYTEA = "bytea";
    public static final String OG_TINYINT = "tinyint";
    public static final String OG_SMALLINT = "int2";
    public static final String OG_SMALLSERIAL = "smallserial";
    public static final String OG_SMALLINT_ARRAY = "_int2";
    public static final String OG_INTEGER = "int4";
    public static final String OG_SERIAL = "serial";
    public static final String OG_INTEGER_ARRAY = "_int4";
    public static final String OG_BIGINT = "int8";
    public static final String OG_BIGSERIAL = "bigserial";
    public static final String OG_BIGINT_ARRAY = "_int8";
    public static final String OG_REAL = "float4";
    public static final String OG_REAL_ARRAY = "_float4";
    public static final String OG_DOUBLE_PRECISION = "float8";
    public static final String OG_DOUBLE_PRECISION_ARRAY = "_float8";
    public static final String OG_NUMERIC = "numeric";
    public static final String OG_MONEY = "money";
    public static final String OG_CHAR = "bpchar";
    public static final String OG_CHARACTER = "character";
    public static final String OG_CHAR_ARRAY = "_bpchar";
    public static final String OG_VARCHAR = "varchar";
    public static final String OG_CHARACTER_VARYING = "character varying";
    public static final String OG_VARCHAR_ARRAY = "_varchar";
    public static final String OG_TEXT = "text";
    public static final String OG_TEXT_ARRAY = "_text";
    public static final String OG_JSON = "json";
    public static final String OG_JSONB = "jsonb";
    public static final String OG_XML = "xml";
    public static final String OG_UUID = "uuid";
    public static final String OG_GEOMETRY = "geometry";
    public static final String OG_GEOGRAPHY = "geography";
    public static final String OG_BIT = "bit";
    public static final String OG_VARBIT = "varbit";
    public static final String OG_DATE = "date";
    public static final String OG_TIME = "time";
    public static final String OG_TIME_TZ = "timetz";
    public static final String OG_TIMESTAMP = "timestamp";
    public static final String OG_TIMESTAMP_TZ = "timestamptz";

    public static final int MAX_PRECISION = 1000;
    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_SCALE = MAX_PRECISION - 1;
    public static final int DEFAULT_SCALE = 18;
    public static final int MAX_TIME_SCALE = 6;
    public static final int MAX_TIMESTAMP_SCALE = 6;
    public static final int MAX_VARCHAR_LENGTH = 10485760;
    public static final long BYTES_2GB = (long) Integer.MAX_VALUE;

    public static final OpenGaussTypeConverter INSTANCE = new OpenGaussTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.OPENGAUSS;
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

        String opengaussType = normalize(typeDefine.getDataType());
        switch (opengaussType) {
            case OG_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case OG_BOOLEAN_ARRAY:
                builder.dataType(ArrayType.BOOLEAN_ARRAY_TYPE);
                break;
            case OG_BIT:
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 1) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(BasicType.STRING_TYPE);
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case OG_VARBIT:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case OG_TINYINT:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case OG_SMALLSERIAL:
            case OG_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case OG_SMALLINT_ARRAY:
                builder.dataType(ArrayType.SHORT_ARRAY_TYPE);
                break;
            case OG_INTEGER:
            case OG_SERIAL:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case OG_INTEGER_ARRAY:
                builder.dataType(ArrayType.INT_ARRAY_TYPE);
                break;
            case OG_BIGINT:
            case OG_BIGSERIAL:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case OG_BIGINT_ARRAY:
                builder.dataType(ArrayType.LONG_ARRAY_TYPE);
                break;
            case OG_REAL:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case OG_REAL_ARRAY:
                builder.dataType(ArrayType.FLOAT_ARRAY_TYPE);
                break;
            case OG_DOUBLE_PRECISION:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case OG_DOUBLE_PRECISION_ARRAY:
                builder.dataType(ArrayType.DOUBLE_ARRAY_TYPE);
                break;
            case OG_NUMERIC:
                DecimalType decimalType;
                if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
                    int scale = typeDefine.getScale() == null ? 0 : typeDefine.getScale();
                    decimalType = new DecimalType(typeDefine.getPrecision().intValue(), scale);
                } else {
                    decimalType = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                }
                builder.dataType(decimalType);
                break;
            case OG_MONEY:
                builder.dataType(new DecimalType(30, 2));
                builder.columnLength(30L);
                builder.scale(2);
                break;
            case OG_CHAR:
            case OG_CHARACTER:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(1L));
                } else {
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                    builder.sourceType(
                            String.format("%s(%s)", opengaussType, typeDefine.getLength()));
                }
                break;
            case OG_VARCHAR:
            case OG_CHARACTER_VARYING:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.sourceType(
                            String.format("%s(%s)", opengaussType, typeDefine.getLength()));
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                }
                break;
            case OG_TEXT:
            case OG_UUID:
            case OG_JSON:
            case OG_JSONB:
            case OG_XML:
            case OG_GEOMETRY:
            case OG_GEOGRAPHY:
                builder.dataType(BasicType.STRING_TYPE);
                if (OG_UUID.equals(opengaussType)) {
                    builder.columnLength(128L);
                } else if (OG_TEXT.equals(opengaussType)) {
                    builder.columnLength(BYTES_2GB);
                }
                break;
            case OG_CHAR_ARRAY:
            case OG_VARCHAR_ARRAY:
            case OG_TEXT_ARRAY:
                builder.dataType(ArrayType.STRING_ARRAY_TYPE);
                break;
            case OG_BYTEA:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case OG_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case OG_TIME:
            case OG_TIME_TZ:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(limitScale(typeDefine.getScale(), MAX_TIME_SCALE, "time"));
                break;
            case OG_TIMESTAMP:
            case OG_TIMESTAMP_TZ:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(limitScale(typeDefine.getScale(), MAX_TIMESTAMP_SCALE, "timestamp"));
                break;
            default:
                throw CommonError.convertToAresTypeError(
                        identifier(), typeDefine.getDataType(), typeDefine.getName());
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
                builder.columnType(OG_BOOLEAN);
                builder.dataType(OG_BOOLEAN);
                break;
            case TINYINT:
                builder.columnType(OG_TINYINT);
                builder.dataType(OG_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(OG_SMALLINT);
                builder.dataType(OG_SMALLINT);
                break;
            case INT:
                builder.columnType(OG_INTEGER);
                builder.dataType(OG_INTEGER);
                break;
            case BIGINT:
                builder.columnType(OG_BIGINT);
                builder.dataType(OG_BIGINT);
                break;
            case FLOAT:
                builder.columnType(OG_REAL);
                builder.dataType(OG_REAL);
                break;
            case DOUBLE:
                builder.columnType(OG_DOUBLE_PRECISION);
                builder.dataType(OG_DOUBLE_PRECISION);
                break;
            case DECIMAL:
                if (column.getSourceType() != null
                        && column.getSourceType().equalsIgnoreCase(OG_MONEY)) {
                    builder.columnType(OG_MONEY);
                    builder.dataType(OG_MONEY);
                } else {
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
                    builder.columnType(String.format("%s(%s,%s)", OG_NUMERIC, precision, scale));
                    builder.dataType(OG_NUMERIC);
                    builder.precision(precision);
                    builder.scale(scale);
                }
                break;
            case BYTES:
                builder.columnType(OG_BYTEA);
                builder.dataType(OG_BYTEA);
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(OG_TEXT);
                    builder.dataType(OG_TEXT);
                } else if (column.getColumnLength() <= MAX_VARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", OG_VARCHAR, column.getColumnLength()));
                    builder.dataType(OG_VARCHAR);
                } else {
                    builder.columnType(OG_TEXT);
                    builder.dataType(OG_TEXT);
                }
                break;
            case DATE:
                builder.columnType(OG_DATE);
                builder.dataType(OG_DATE);
                break;
            case TIME:
                Integer timeScale = column.getScale();
                if (timeScale != null && timeScale > MAX_TIME_SCALE) {
                    timeScale = MAX_TIME_SCALE;
                }
                if (timeScale != null && timeScale > 0) {
                    builder.columnType(String.format("%s(%s)", OG_TIME, timeScale));
                } else {
                    builder.columnType(OG_TIME);
                }
                builder.dataType(OG_TIME);
                builder.scale(timeScale);
                break;
            case TIMESTAMP:
                Integer timestampScale = column.getScale();
                if (timestampScale != null && timestampScale > MAX_TIMESTAMP_SCALE) {
                    timestampScale = MAX_TIMESTAMP_SCALE;
                }
                if (timestampScale != null && timestampScale > 0) {
                    builder.columnType(String.format("%s(%s)", OG_TIMESTAMP, timestampScale));
                } else {
                    builder.columnType(OG_TIMESTAMP);
                }
                builder.dataType(OG_TIMESTAMP);
                builder.scale(timestampScale);
                break;
            case ARRAY:
                ArrayType arrayType = (ArrayType) column.getDataType();
                AresDataType elementType = arrayType.getElementType();
                switch (elementType.getSqlType()) {
                    case BOOLEAN:
                        builder.columnType(OG_BOOLEAN_ARRAY);
                        builder.dataType(OG_BOOLEAN_ARRAY);
                        break;
                    case TINYINT:
                    case SMALLINT:
                        builder.columnType(OG_SMALLINT_ARRAY);
                        builder.dataType(OG_SMALLINT_ARRAY);
                        break;
                    case INT:
                        builder.columnType(OG_INTEGER_ARRAY);
                        builder.dataType(OG_INTEGER_ARRAY);
                        break;
                    case BIGINT:
                        builder.columnType(OG_BIGINT_ARRAY);
                        builder.dataType(OG_BIGINT_ARRAY);
                        break;
                    case FLOAT:
                        builder.columnType(OG_REAL_ARRAY);
                        builder.dataType(OG_REAL_ARRAY);
                        break;
                    case DOUBLE:
                        builder.columnType(OG_DOUBLE_PRECISION_ARRAY);
                        builder.dataType(OG_DOUBLE_PRECISION_ARRAY);
                        break;
                    case BYTES:
                        builder.columnType(OG_BYTEA);
                        builder.dataType(OG_BYTEA);
                        break;
                    case STRING:
                        builder.columnType(OG_TEXT_ARRAY);
                        builder.dataType(OG_TEXT_ARRAY);
                        break;
                    default:
                        throw CommonError.convertToConnectorTypeError(
                                DatabaseIdentifier.OPENGAUSS,
                                elementType.getSqlType().name(),
                                column.getName());
                }
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.OPENGAUSS,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }

    private Integer limitScale(Integer scale, int maxScale, String typeName) {
        if (scale != null && scale > maxScale) {
            log.warn(
                    "The scale of {} type is larger than {}, it will be truncated to {}",
                    typeName,
                    maxScale,
                    maxScale);
            return maxScale;
        }
        return scale;
    }

    static String normalize(String dataType) {
        String type = dataType.toLowerCase().trim();
        int paren = type.indexOf('(');
        if (paren > 0) {
            type = type.substring(0, paren).trim();
        }
        if (type.endsWith(" without time zone")) {
            type = type.substring(0, type.length() - " without time zone".length()).trim();
        } else if (type.endsWith(" with time zone")) {
            if (type.startsWith("timestamp")) {
                return OG_TIMESTAMP_TZ;
            }
            if (type.startsWith("time")) {
                return OG_TIME_TZ;
            }
        }
        switch (type) {
            case "boolean":
                return OG_BOOLEAN;
            case "int1":
            case OG_TINYINT:
                return OG_TINYINT;
            case "smallint":
                return OG_SMALLINT;
            case "integer":
            case "int":
                return OG_INTEGER;
            case "bigint":
                return OG_BIGINT;
            case "real":
                return OG_REAL;
            case "double precision":
            case "double":
            case "float":
                return OG_DOUBLE_PRECISION;
            case "decimal":
            case "number":
                return OG_NUMERIC;
            case "char":
            case "character":
            case "nchar":
                return OG_CHAR;
            case "character varying":
            case "varchar2":
            case "nvarchar":
            case "nvarchar2":
                return OG_VARCHAR;
            case "datetime":
                return OG_TIMESTAMP;
            case "clob":
                return OG_TEXT;
            case "blob":
            case "binary":
            case "varbinary":
                return OG_BYTEA;
            case "bit varying":
                return OG_VARBIT;
            default:
                return type;
        }
    }
}
