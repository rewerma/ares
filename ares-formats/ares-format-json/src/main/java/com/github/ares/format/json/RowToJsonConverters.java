package com.github.ares.format.json;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.MapType;
import com.github.ares.api.table.type.SqlType;
import com.github.ares.com.fasterxml.jackson.databind.JsonNode;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ares.com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.ares.com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.ares.format.json.exception.AresJsonFormatException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

import com.github.ares.common.exceptions.CommonErrorCode;

public class RowToJsonConverters implements Serializable {

    private static final long serialVersionUID = 6988876688930916940L;

    public RowToJsonConverter createConverter(AresDataType<?> type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    private RowToJsonConverter wrapIntoNullableConverter(RowToJsonConverter converter) {
        return new RowToJsonConverter() {
            @Override
            public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                if (value == null) {
                    return mapper.getNodeFactory().nullNode();
                }
                return converter.convert(mapper, reuse, value);
            }
        };
    }

    private RowToJsonConverter createNotNullConverter(AresDataType<?> type) {
        SqlType sqlType = type.getSqlType();
        switch (sqlType) {
            case ROW:
                return createRowConverter((AresRowType) type);
            case NULL:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return null;
                    }
                };
            case BOOLEAN:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().booleanNode((Boolean) value);
                    }
                };
            case TINYINT:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((byte) value);
                    }
                };
            case SMALLINT:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((short) value);
                    }
                };
            case INT:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((int) value);
                    }
                };
            case BIGINT:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((long) value);
                    }
                };
            case FLOAT:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((float) value);
                    }
                };
            case DOUBLE:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((double) value);
                    }
                };
            case DECIMAL:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().numberNode((BigDecimal) value);
                    }
                };
            case BYTES:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().binaryNode((byte[]) value);
                    }
                };
            case STRING:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory().textNode((String) value);
                    }
                };
            case DATE:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory()
                                .textNode(ISO_LOCAL_DATE.format((LocalDate) value));
                    }
                };
            case TIME:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory()
                                .textNode(TimeFormat.TIME_FORMAT.format((LocalTime) value));
                    }
                };
            case TIMESTAMP:
                return new RowToJsonConverter() {
                    @Override
                    public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                        return mapper.getNodeFactory()
                                .textNode(ISO_LOCAL_DATE_TIME.format((LocalDateTime) value));
                    }
                };
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case MAP:
                MapType mapType = (MapType) type;
                return createMapConverter(
                        mapType.toString(), mapType.getKeyType(), mapType.getValueType());
            default:
                throw new AresJsonFormatException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "unsupported parse type: " + type);
        }
    }

    private RowToJsonConverter createRowConverter(AresRowType rowType) {
        final RowToJsonConverter[] fieldConverters =
                Arrays.stream(rowType.getFieldTypes())
                        .map(
                                new Function<AresDataType<?>, Object>() {
                                    @Override
                                    public Object apply(AresDataType<?> aresDataType) {
                                        return createConverter(aresDataType);
                                    }
                                })
                        .toArray(
                                new IntFunction<RowToJsonConverter[]>() {
                                    @Override
                                    public RowToJsonConverter[] apply(int value) {
                                        return new RowToJsonConverter[value];
                                    }
                                });
        final String[] fieldNames = rowType.getFieldNames();
        final int arity = fieldNames.length;

        return new RowToJsonConverter() {
            @Override
            public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                ObjectNode node;

                // reuse could be a NullNode if last record is null.
                if (reuse == null || reuse.isNull()) {
                    node = mapper.createObjectNode();
                } else {
                    node = (ObjectNode) reuse;
                }

                for (int i = 0; i < arity; i++) {
                    String fieldName = fieldNames[i];
                    AresRow row = (AresRow) value;
                    node.set(
                            fieldName,
                            fieldConverters[i].convert(
                                    mapper, node.get(fieldName), row.getField(i)));
                }

                return node;
            }
        };
    }

    private RowToJsonConverter createArrayConverter(ArrayType arrayType) {
        final RowToJsonConverter elementConverter = createConverter(arrayType.getElementType());
        return new RowToJsonConverter() {
            @Override
            public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                ArrayNode node;

                // reuse could be a NullNode if last record is null.
                if (reuse == null || reuse.isNull()) {
                    node = mapper.createArrayNode();
                } else {
                    node = (ArrayNode) reuse;
                    node.removeAll();
                }

                Object[] arrayData = (Object[]) value;
                int numElements = arrayData.length;
                for (int i = 0; i < numElements; i++) {
                    Object element = arrayData[i];
                    node.add(elementConverter.convert(mapper, null, element));
                }

                return node;
            }
        };
    }

    private RowToJsonConverter createMapConverter(
            String typeSummary, AresDataType<?> keyType, AresDataType<?> valueType) {
        if (!SqlType.STRING.equals(keyType.getSqlType())) {
            throw new AresJsonFormatException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "JSON format doesn't support non-string as key type of map. The type is: "
                            + typeSummary);
        }

        final RowToJsonConverter valueConverter = createConverter(valueType);
        return new RowToJsonConverter() {
            @Override
            public JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value) {
                ObjectNode node;

                // reuse could be a NullNode if last record is null.
                if (reuse == null || reuse.isNull()) {
                    node = mapper.createObjectNode();
                } else {
                    node = (ObjectNode) reuse;
                    node.removeAll();
                }

                Map<String, ?> mapData = (Map) value;
                for (Map.Entry<String, ?> entry : mapData.entrySet()) {
                    String fieldName = entry.getKey();
                    node.set(
                            fieldName,
                            valueConverter.convert(mapper, node.get(fieldName), entry.getValue()));
                }

                return node;
            }
        };
    }

    public interface RowToJsonConverter extends Serializable {
        JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value);
    }
}
