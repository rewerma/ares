package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.PhysicalColumn;
import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.api.table.connector.TypeConverter;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.common.exceptions.AresException;
import com.google.auto.service.AutoService;

@AutoService(TypeConverter.class)
public class StarRocksTypeConverter implements TypeConverter<BasicTypeDefine<String>> {
    public static final int MAX_VARCHAR_LENGTH = 65533;
    public static final int MAX_PRECISION = StarRocksTypeMapper.MAX_PRECISION;
    public static final int MAX_SCALE = 30;

    @Override
    public String identifier() {
        return DatabaseIdentifier.STARROCKS;
    }

    @Override
    public Column convert(BasicTypeDefine<String> typeDefine) {
        String columnType = typeDefine.getColumnType();
        if (columnType == null) {
            columnType = typeDefine.getDataType();
        }
        StarRocksTypeMapper mapper = new StarRocksTypeMapper();
        int precision =
                typeDefine.getPrecision() == null ? 0 : typeDefine.getPrecision().intValue();
        int scale = typeDefine.getScale() == null ? 0 : typeDefine.getScale();
        return PhysicalColumn.builder()
                .name(typeDefine.getName())
                .sourceType(columnType)
                .nullable(typeDefine.isNullable())
                .defaultValue(typeDefine.getDefaultValue())
                .comment(typeDefine.getComment())
                .dataType(mapper.map(columnType, precision, scale, typeDefine.getName()))
                .columnLength(typeDefine.getLength())
                .scale(typeDefine.getScale())
                .build();
    }

    @Override
    public BasicTypeDefine<String> reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder<String> builder =
                BasicTypeDefine.<String>builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());
        switch (column.getDataType().getSqlType()) {
            case NULL:
                return typed(builder, "NULL");
            case BOOLEAN:
                return typed(builder, "BOOLEAN");
            case TINYINT:
                return typed(builder, "TINYINT");
            case SMALLINT:
                return typed(builder, "SMALLINT");
            case INT:
                return typed(builder, "INT");
            case BIGINT:
                return typed(builder, "BIGINT");
            case FLOAT:
                return typed(builder, "FLOAT");
            case DOUBLE:
                return typed(builder, "DOUBLE");
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (precision <= 0) {
                    precision = MAX_PRECISION;
                    scale = StarRocksTypeMapper.DEFAULT_SCALE;
                } else if (precision > MAX_PRECISION) {
                    scale = Math.max(0, scale - (precision - MAX_PRECISION));
                    precision = MAX_PRECISION;
                }
                if (scale < 0) {
                    scale = 0;
                } else if (scale > MAX_SCALE) {
                    scale = MAX_SCALE;
                }
                String decimal = String.format("DECIMAL(%s,%s)", precision, scale);
                return builder.columnType(decimal)
                        .dataType("DECIMAL")
                        .nativeType(decimal)
                        .precision((long) precision)
                        .scale(scale)
                        .build();
            case BYTES:
            case STRING:
                Long length = column.getColumnLength();
                if (length == null || length <= 0 || length > MAX_VARCHAR_LENGTH) {
                    return typed(builder, "STRING");
                }
                String varchar = String.format("VARCHAR(%s)", length);
                return builder.columnType(varchar)
                        .dataType("VARCHAR")
                        .nativeType(varchar)
                        .length(length)
                        .build();
            case DATE:
                return typed(builder, "DATE");
            case TIME:
            case TIMESTAMP:
                return typed(builder, "DATETIME");
            default:
                throw new AresException(
                        "StarRocks cannot reconvert type "
                                + column.getDataType().getSqlType()
                                + " of column "
                                + column.getName());
        }
    }

    private static BasicTypeDefine<String> typed(
            BasicTypeDefine.BasicTypeDefineBuilder<String> builder, String type) {
        return builder.columnType(type).dataType(type).nativeType(type).build();
    }
}
