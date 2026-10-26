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

/** KingbaseES type mapping. PostgreSQL mode uses the same internal type names as Postgres. */
@AutoService(TypeConverter.class)
public class KingbaseTypeConverter implements TypeConverter<BasicTypeDefine> {
    private static final Logger log = LoggerFactory.getLogger(KingbaseTypeConverter.class);

    public static final String KB_BOOLEAN = "bool";
    public static final String KB_BOOLEAN_ARRAY = "_bool";
    public static final String KB_BYTEA = "bytea";
    public static final String KB_TINYINT = "tinyint";
    public static final String KB_SMALLINT = "int2";
    public static final String KB_SMALLSERIAL = "smallserial";
    public static final String KB_SMALLINT_ARRAY = "_int2";
    public static final String KB_INTEGER = "int4";
    public static final String KB_SERIAL = "serial";
    public static final String KB_INTEGER_ARRAY = "_int4";
    public static final String KB_BIGINT = "int8";
    public static final String KB_BIGSERIAL = "bigserial";
    public static final String KB_BIGINT_ARRAY = "_int8";
    public static final String KB_REAL = "float4";
    public static final String KB_REAL_ARRAY = "_float4";
    public static final String KB_DOUBLE_PRECISION = "float8";
    public static final String KB_DOUBLE_PRECISION_ARRAY = "_float8";
    public static final String KB_NUMERIC = "numeric";
    public static final String KB_MONEY = "money";
    public static final String KB_CHAR = "bpchar";
    public static final String KB_CHARACTER = "character";
    public static final String KB_CHAR_ARRAY = "_bpchar";
    public static final String KB_VARCHAR = "varchar";
    public static final String KB_CHARACTER_VARYING = "character varying";
    public static final String KB_VARCHAR_ARRAY = "_varchar";
    public static final String KB_TEXT = "text";
    public static final String KB_TEXT_ARRAY = "_text";
    public static final String KB_JSON = "json";
    public static final String KB_JSONB = "jsonb";
    public static final String KB_XML = "xml";
    public static final String KB_UUID = "uuid";
    public static final String KB_GEOMETRY = "geometry";
    public static final String KB_GEOGRAPHY = "geography";
    public static final String KB_BIT = "bit";
    public static final String KB_VARBIT = "varbit";
    public static final String KB_DATE = "date";
    public static final String KB_TIME = "time";
    public static final String KB_TIME_TZ = "timetz";
    public static final String KB_TIMESTAMP = "timestamp";
    public static final String KB_TIMESTAMP_TZ = "timestamptz";

    public static final int MAX_PRECISION = 1000;
    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_SCALE = MAX_PRECISION - 1;
    public static final int DEFAULT_SCALE = 18;
    public static final int MAX_TIME_SCALE = 6;
    public static final int MAX_TIMESTAMP_SCALE = 6;
    public static final int MAX_VARCHAR_LENGTH = 10485760;
    public static final long BYTES_2GB = (long) Integer.MAX_VALUE;

    public static final KingbaseTypeConverter INSTANCE = new KingbaseTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.KINGBASE;
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

        String kingbaseType = normalize(typeDefine.getDataType());
        switch (kingbaseType) {
            case KB_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case KB_BOOLEAN_ARRAY:
                builder.dataType(ArrayType.BOOLEAN_ARRAY_TYPE);
                break;
            case KB_BIT:
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 1) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(BasicType.STRING_TYPE);
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case KB_VARBIT:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case KB_TINYINT:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case KB_SMALLSERIAL:
            case KB_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case KB_SMALLINT_ARRAY:
                builder.dataType(ArrayType.SHORT_ARRAY_TYPE);
                break;
            case KB_INTEGER:
            case KB_SERIAL:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case KB_INTEGER_ARRAY:
                builder.dataType(ArrayType.INT_ARRAY_TYPE);
                break;
            case KB_BIGINT:
            case KB_BIGSERIAL:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case KB_BIGINT_ARRAY:
                builder.dataType(ArrayType.LONG_ARRAY_TYPE);
                break;
            case KB_REAL:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case KB_REAL_ARRAY:
                builder.dataType(ArrayType.FLOAT_ARRAY_TYPE);
                break;
            case KB_DOUBLE_PRECISION:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case KB_DOUBLE_PRECISION_ARRAY:
                builder.dataType(ArrayType.DOUBLE_ARRAY_TYPE);
                break;
            case KB_NUMERIC:
                DecimalType decimalType;
                if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
                    int scale = typeDefine.getScale() == null ? 0 : typeDefine.getScale();
                    decimalType = new DecimalType(typeDefine.getPrecision().intValue(), scale);
                } else {
                    decimalType = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                }
                builder.dataType(decimalType);
                break;
            case KB_MONEY:
                builder.dataType(new DecimalType(30, 2));
                builder.columnLength(30L);
                builder.scale(2);
                break;
            case KB_CHAR:
            case KB_CHARACTER:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(1L));
                } else {
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                    builder.sourceType(
                            String.format("%s(%s)", kingbaseType, typeDefine.getLength()));
                }
                break;
            case KB_VARCHAR:
            case KB_CHARACTER_VARYING:
                builder.dataType(BasicType.STRING_TYPE);
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.sourceType(
                            String.format("%s(%s)", kingbaseType, typeDefine.getLength()));
                    builder.columnLength(TypeDefineUtils.charTo4ByteLength(typeDefine.getLength()));
                }
                break;
            case KB_TEXT:
            case KB_UUID:
            case KB_JSON:
            case KB_JSONB:
            case KB_XML:
            case KB_GEOMETRY:
            case KB_GEOGRAPHY:
                builder.dataType(BasicType.STRING_TYPE);
                if (KB_UUID.equals(kingbaseType)) {
                    builder.columnLength(128L);
                } else if (KB_TEXT.equals(kingbaseType)) {
                    builder.columnLength(BYTES_2GB);
                }
                break;
            case KB_CHAR_ARRAY:
            case KB_VARCHAR_ARRAY:
            case KB_TEXT_ARRAY:
                builder.dataType(ArrayType.STRING_ARRAY_TYPE);
                break;
            case KB_BYTEA:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                if (typeDefine.getLength() != null && typeDefine.getLength() > 0) {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case KB_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case KB_TIME:
            case KB_TIME_TZ:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(limitScale(typeDefine.getScale(), MAX_TIME_SCALE, "time"));
                break;
            case KB_TIMESTAMP:
            case KB_TIMESTAMP_TZ:
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
                builder.columnType(KB_BOOLEAN);
                builder.dataType(KB_BOOLEAN);
                break;
            case TINYINT:
                builder.columnType(KB_TINYINT);
                builder.dataType(KB_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(KB_SMALLINT);
                builder.dataType(KB_SMALLINT);
                break;
            case INT:
                builder.columnType(KB_INTEGER);
                builder.dataType(KB_INTEGER);
                break;
            case BIGINT:
                builder.columnType(KB_BIGINT);
                builder.dataType(KB_BIGINT);
                break;
            case FLOAT:
                builder.columnType(KB_REAL);
                builder.dataType(KB_REAL);
                break;
            case DOUBLE:
                builder.columnType(KB_DOUBLE_PRECISION);
                builder.dataType(KB_DOUBLE_PRECISION);
                break;
            case DECIMAL:
                if (column.getSourceType() != null
                        && column.getSourceType().equalsIgnoreCase(KB_MONEY)) {
                    builder.columnType(KB_MONEY);
                    builder.dataType(KB_MONEY);
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
                    builder.columnType(String.format("%s(%s,%s)", KB_NUMERIC, precision, scale));
                    builder.dataType(KB_NUMERIC);
                    builder.precision(precision);
                    builder.scale(scale);
                }
                break;
            case BYTES:
                builder.columnType(KB_BYTEA);
                builder.dataType(KB_BYTEA);
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(KB_TEXT);
                    builder.dataType(KB_TEXT);
                } else if (column.getColumnLength() <= MAX_VARCHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", KB_VARCHAR, column.getColumnLength()));
                    builder.dataType(KB_VARCHAR);
                } else {
                    builder.columnType(KB_TEXT);
                    builder.dataType(KB_TEXT);
                }
                break;
            case DATE:
                builder.columnType(KB_DATE);
                builder.dataType(KB_DATE);
                break;
            case TIME:
                Integer timeScale = column.getScale();
                if (timeScale != null && timeScale > MAX_TIME_SCALE) {
                    timeScale = MAX_TIME_SCALE;
                }
                if (timeScale != null && timeScale > 0) {
                    builder.columnType(String.format("%s(%s)", KB_TIME, timeScale));
                } else {
                    builder.columnType(KB_TIME);
                }
                builder.dataType(KB_TIME);
                builder.scale(timeScale);
                break;
            case TIMESTAMP:
                Integer timestampScale = column.getScale();
                if (timestampScale != null && timestampScale > MAX_TIMESTAMP_SCALE) {
                    timestampScale = MAX_TIMESTAMP_SCALE;
                }
                if (timestampScale != null && timestampScale > 0) {
                    builder.columnType(String.format("%s(%s)", KB_TIMESTAMP, timestampScale));
                } else {
                    builder.columnType(KB_TIMESTAMP);
                }
                builder.dataType(KB_TIMESTAMP);
                builder.scale(timestampScale);
                break;
            case ARRAY:
                ArrayType arrayType = (ArrayType) column.getDataType();
                AresDataType elementType = arrayType.getElementType();
                switch (elementType.getSqlType()) {
                    case BOOLEAN:
                        builder.columnType(KB_BOOLEAN_ARRAY);
                        builder.dataType(KB_BOOLEAN_ARRAY);
                        break;
                    case TINYINT:
                    case SMALLINT:
                        builder.columnType(KB_SMALLINT_ARRAY);
                        builder.dataType(KB_SMALLINT_ARRAY);
                        break;
                    case INT:
                        builder.columnType(KB_INTEGER_ARRAY);
                        builder.dataType(KB_INTEGER_ARRAY);
                        break;
                    case BIGINT:
                        builder.columnType(KB_BIGINT_ARRAY);
                        builder.dataType(KB_BIGINT_ARRAY);
                        break;
                    case FLOAT:
                        builder.columnType(KB_REAL_ARRAY);
                        builder.dataType(KB_REAL_ARRAY);
                        break;
                    case DOUBLE:
                        builder.columnType(KB_DOUBLE_PRECISION_ARRAY);
                        builder.dataType(KB_DOUBLE_PRECISION_ARRAY);
                        break;
                    case BYTES:
                        builder.columnType(KB_BYTEA);
                        builder.dataType(KB_BYTEA);
                        break;
                    case STRING:
                        builder.columnType(KB_TEXT_ARRAY);
                        builder.dataType(KB_TEXT_ARRAY);
                        break;
                    default:
                        throw CommonError.convertToConnectorTypeError(
                                DatabaseIdentifier.KINGBASE,
                                elementType.getSqlType().name(),
                                column.getName());
                }
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.KINGBASE,
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
                return KB_TIMESTAMP_TZ;
            }
            if (type.startsWith("time")) {
                return KB_TIME_TZ;
            }
        }
        switch (type) {
            case "boolean":
                return KB_BOOLEAN;
            case "int1":
            case KB_TINYINT:
                return KB_TINYINT;
            case "smallint":
                return KB_SMALLINT;
            case "integer":
            case "int":
                return KB_INTEGER;
            case "bigint":
                return KB_BIGINT;
            case "real":
                return KB_REAL;
            case "double precision":
            case "double":
            case "float":
                return KB_DOUBLE_PRECISION;
            case "decimal":
            case "number":
                return KB_NUMERIC;
            case "char":
            case "character":
            case "nchar":
                return KB_CHAR;
            case "character varying":
            case "varchar2":
            case "nvarchar":
            case "nvarchar2":
                return KB_VARCHAR;
            case "datetime":
                return KB_TIMESTAMP;
            case "clob":
                return KB_TEXT;
            case "blob":
            case "binary":
            case "varbinary":
                return KB_BYTEA;
            case "bit varying":
                return KB_VARBIT;
            default:
                return type;
        }
    }
}
