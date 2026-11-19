package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.PhysicalColumn;
import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.api.table.connector.TypeConverter;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.common.exceptions.CommonError;
import com.google.auto.service.AutoService;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;

@AutoService(TypeConverter.class)
public class ClickHouseTypeConverter implements TypeConverter<BasicTypeDefine> {

    public static final String CH_BOOL = "Bool";
    public static final String CH_INT8 = "Int8";
    public static final String CH_INT16 = "Int16";
    public static final String CH_INT32 = "Int32";
    public static final String CH_INT64 = "Int64";
    public static final String CH_FLOAT32 = "Float32";
    public static final String CH_FLOAT64 = "Float64";
    public static final String CH_DECIMAL = "Decimal";
    public static final String CH_STRING = "String";
    public static final String CH_DATE = "Date";
    public static final String CH_DATETIME = "DateTime";

    public static final int DEFAULT_PRECISION = 38;
    public static final int DEFAULT_SCALE = 9;
    public static final int MAX_PRECISION = 76;

    public static final ClickHouseTypeConverter INSTANCE = new ClickHouseTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.CLICKHOUSE;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable() || isNullable(typeDefine.getDataType()))
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        String unwrapped = unwrap(typeDefine.getDataType());
        switch (baseTypeName(unwrapped)) {
            case "BOOL":
            case "BOOLEAN":
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case "INT8":
            case "TINYINT":
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case "INT16":
            case "SMALLINT":
            case "UINT8":
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case "INT32":
            case "INT":
            case "INTEGER":
            case "UINT16":
                builder.dataType(BasicType.INT_TYPE);
                break;
            case "INT64":
            case "BIGINT":
            case "UINT32":
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case "UINT64":
            case "INT128":
            case "UINT128":
            case "INT256":
            case "UINT256":
                builder.dataType(new DecimalType(38, 0));
                builder.columnLength(38L);
                builder.scale(0);
                break;
            case "FLOAT32":
            case "FLOAT":
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case "FLOAT64":
            case "DOUBLE":
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case "DECIMAL":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
            case "DECIMAL256":
                DecimalType decimalType = resolveDecimal(unwrapped, typeDefine);
                builder.dataType(decimalType);
                builder.columnLength((long) decimalType.getPrecision());
                builder.scale(decimalType.getScale());
                break;
            case "DATE":
            case "DATE32":
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case "DATETIME":
            case "DATETIME64":
            case "TIMESTAMP":
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case "FIXEDSTRING":
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(firstNumericParam(unwrapped, typeDefine.getLength()));
                break;
            case "STRING":
            case "UUID":
            case "IPV4":
            case "IPV6":
            case "JSON":
            case "OBJECT":
            case "ENUM8":
            case "ENUM16":
            case "ENUM":
            case "ARRAY":
            case "MAP":
            case "TUPLE":
            case "NESTED":
            case "NOTHING":
                builder.dataType(BasicType.STRING_TYPE);
                break;
            default:
                throw CommonError.convertToAresTypeError(
                        DatabaseIdentifier.CLICKHOUSE,
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
        String columnType;
        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                columnType = CH_BOOL;
                builder.dataType(CH_BOOL);
                break;
            case TINYINT:
                columnType = CH_INT8;
                builder.dataType(CH_INT8);
                break;
            case SMALLINT:
                columnType = CH_INT16;
                builder.dataType(CH_INT16);
                break;
            case INT:
                columnType = CH_INT32;
                builder.dataType(CH_INT32);
                break;
            case BIGINT:
                columnType = CH_INT64;
                builder.dataType(CH_INT64);
                break;
            case FLOAT:
                columnType = CH_FLOAT32;
                builder.dataType(CH_FLOAT32);
                break;
            case DOUBLE:
                columnType = CH_FLOAT64;
                builder.dataType(CH_FLOAT64);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                int precision =
                        decimalType.getPrecision() > 0
                                ? Math.min(decimalType.getPrecision(), MAX_PRECISION)
                                : DEFAULT_PRECISION;
                int scale = decimalType.getScale();
                if (scale < 0) {
                    scale = 0;
                } else if (scale > precision) {
                    scale = precision;
                }
                columnType = String.format("%s(%s,%s)", CH_DECIMAL, precision, scale);
                builder.dataType(CH_DECIMAL);
                builder.precision((long) precision);
                builder.scale(scale);
                break;
            case STRING:
            case BYTES:
                columnType = CH_STRING;
                builder.dataType(CH_STRING);
                builder.length(column.getColumnLength());
                break;
            case DATE:
                columnType = CH_DATE;
                builder.dataType(CH_DATE);
                break;
            case TIME:
            case TIMESTAMP:
                columnType = CH_DATETIME;
                builder.dataType(CH_DATETIME);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.CLICKHOUSE,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        if (column.isNullable() && !columnType.toUpperCase(Locale.ROOT).startsWith("NULLABLE(")) {
            columnType = "Nullable(" + columnType + ")";
        }
        builder.columnType(columnType);
        return builder.build();
    }

    static String unwrap(String type) {
        if (StringUtils.isBlank(type)) {
            return type;
        }
        String current = type.trim();
        while (true) {
            String upper = current.toUpperCase(Locale.ROOT);
            if ((upper.startsWith("NULLABLE(") || upper.startsWith("LOWCARDINALITY("))
                    && current.endsWith(")")) {
                current = current.substring(current.indexOf('(') + 1, current.length() - 1).trim();
                continue;
            }
            break;
        }
        return current;
    }

    static boolean isNullable(String type) {
        if (StringUtils.isBlank(type)) {
            return false;
        }
        String current = type.trim();
        while (true) {
            String upper = current.toUpperCase(Locale.ROOT);
            if (upper.startsWith("NULLABLE(") && current.endsWith(")")) {
                return true;
            }
            if (upper.startsWith("LOWCARDINALITY(") && current.endsWith(")")) {
                current = current.substring(current.indexOf('(') + 1, current.length() - 1).trim();
                continue;
            }
            return false;
        }
    }

    private static String baseTypeName(String type) {
        if (StringUtils.isBlank(type)) {
            return "";
        }
        int idx = type.indexOf('(');
        String base = idx > 0 ? type.substring(0, idx) : type;
        return base.trim().toUpperCase(Locale.ROOT);
    }

    private static DecimalType resolveDecimal(String unwrapped, BasicTypeDefine typeDefine) {
        if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
            int scale = typeDefine.getScale() == null ? 0 : typeDefine.getScale();
            return new DecimalType(typeDefine.getPrecision().intValue(), Math.max(scale, 0));
        }
        int start = unwrapped.indexOf('(');
        int end = unwrapped.lastIndexOf(')');
        if (start > 0 && end > start) {
            String[] parts = unwrapped.substring(start + 1, end).split(",");
            try {
                if (parts.length >= 2) {
                    return new DecimalType(
                            Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim()));
                }
                if (parts.length == 1) {
                    return new DecimalType(DEFAULT_PRECISION, Integer.parseInt(parts[0].trim()));
                }
            } catch (NumberFormatException ignored) {
                return new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
            }
        }
        return new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
    }

    private static Long firstNumericParam(String unwrapped, Long fallback) {
        int start = unwrapped.indexOf('(');
        int end = unwrapped.lastIndexOf(')');
        if (start > 0 && end > start) {
            try {
                return Long.parseLong(unwrapped.substring(start + 1, end).split(",")[0].trim());
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        return fallback;
    }
}
