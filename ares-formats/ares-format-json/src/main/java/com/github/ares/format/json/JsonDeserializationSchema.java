package com.github.ares.format.json;

import com.github.ares.api.serialization.DeserializationSchema;
import com.github.ares.api.source.Collector;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.CompositeType;
import com.github.ares.api.table.type.SqlType;
import com.github.ares.com.fasterxml.jackson.core.JsonProcessingException;
import com.github.ares.com.fasterxml.jackson.core.json.JsonReadFeature;
import com.github.ares.com.fasterxml.jackson.databind.DeserializationFeature;
import com.github.ares.com.fasterxml.jackson.databind.JsonNode;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ares.com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.ares.com.fasterxml.jackson.databind.node.NullNode;
import com.github.ares.common.exceptions.CommonError;
import com.github.ares.format.json.exception.AresJsonFormatException;

import com.github.ares.common.exceptions.CommonErrorCode;

import java.io.IOException;

import static com.github.ares.com.google.common.base.Preconditions.checkNotNull;

public class JsonDeserializationSchema implements DeserializationSchema<AresRow> {
    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "Common";

    /** Flag indicating whether to fail if a field is missing. */
    private final boolean failOnMissingField;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** The row type of the produced {@link AresRow}. */
    private final AresRowType rowType;

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of internal data structures.
     */
    private final JsonToRowConverters.JsonToObjectConverter runtimeConverter;

    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonDeserializationSchema(
            boolean failOnMissingField, boolean ignoreParseErrors, AresRowType rowType) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new AresJsonFormatException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.rowType = checkNotNull(rowType);
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.runtimeConverter =
                new JsonToRowConverters(failOnMissingField, ignoreParseErrors)
                        .createRowConverter(checkNotNull(rowType));

        if (hasDecimalType(rowType)) {
            objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        }
        objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
    }

    private static boolean hasDecimalType(AresDataType<?> dataType) {
        if (dataType.getSqlType() == SqlType.DECIMAL) {
            return true;
        }
        if (dataType instanceof CompositeType) {
            CompositeType<?> compositeType = (CompositeType<?>) dataType;
            for (AresDataType<?> child : compositeType.getChildren()) {
                if (hasDecimalType(child)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public AresRow deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        return convertJsonNode(convertBytes(message));
    }

    public AresRow deserialize(String message) throws IOException {
        if (message == null) {
            return null;
        }
        return convertJsonNode(convert(message));
    }

    public void collect(byte[] message, Collector<AresRow> out) throws IOException {
        JsonNode jsonNode = convertBytes(message);
        if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            for (int i = 0; i < arrayNode.size(); i++) {
                AresRow deserialize = convertJsonNode(arrayNode.get(i));
                out.collect(deserialize);
            }
        } else {
            AresRow deserialize = convertJsonNode(jsonNode);
            out.collect(deserialize);
        }
    }

    private AresRow convertJsonNode(JsonNode jsonNode) {
        if (jsonNode.isNull()) {
            return null;
        }
        try {
            return (AresRow) runtimeConverter.convert(jsonNode, null);
        } catch (RuntimeException e) {
            if (ignoreParseErrors) {
                return null;
            }
            throw CommonError.jsonOperationError(FORMAT, jsonNode.toString(), e);
        }
    }

    public JsonNode deserializeToJsonNode(byte[] message) throws IOException {
        return objectMapper.readTree(message);
    }

    public AresRow convertToRowData(JsonNode message) {
        return (AresRow) runtimeConverter.convert(message, null);
    }

    private JsonNode convertBytes(byte[] message) {
        try {
            return objectMapper.readTree(message);
        } catch (IOException | RuntimeException e) {
            if (ignoreParseErrors) {
                return NullNode.getInstance();
            }
            throw CommonError.jsonOperationError(FORMAT, new String(message), e);
        }
    }

    private JsonNode convert(String message) {
        try {
            return objectMapper.readTree(message);
        } catch (JsonProcessingException | RuntimeException e) {
            if (ignoreParseErrors) {
                return NullNode.getInstance();
            }
            throw CommonError.jsonOperationError(FORMAT, new String(message), e);
        }
    }

    @Override
    public AresRowType getProducedType() {
        return this.rowType;
    }
}
