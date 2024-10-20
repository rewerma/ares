package com.github.ares.format.json;

import com.github.ares.api.serialization.SerializationSchema;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ares.com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.ares.common.exceptions.CommonError;
import lombok.Getter;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static com.github.ares.com.google.common.base.Preconditions.checkNotNull;


public class JsonSerializationSchema implements SerializationSchema {

    public static final String FORMAT = "Common";
    /** RowType to generate the runtime converter. */
    private final AresRowType rowType;

    /** Reusable object node. */
    private transient ObjectNode node;

    /** Object mapper that is used to create output JSON objects. */
    @Getter private final ObjectMapper mapper = new ObjectMapper();

    private final Charset charset;

    private final RowToJsonConverters.RowToJsonConverter runtimeConverter;

    public JsonSerializationSchema(AresRowType rowType) {
        this(rowType, StandardCharsets.UTF_8);
    }

    public JsonSerializationSchema(AresRowType rowType, Charset charset) {
        this.rowType = rowType;
        this.runtimeConverter = new RowToJsonConverters().createConverter(checkNotNull(rowType));
        this.charset = charset;
    }

    @Override
    public byte[] serialize(AresRow row) {
        if (node == null) {
            node = mapper.createObjectNode();
        }

        try {
            runtimeConverter.convert(mapper, node, row);
            return mapper.writeValueAsString(node).getBytes(charset);
        } catch (Throwable t) {
            throw CommonError.jsonOperationError(FORMAT, row.toString(), t);
        }
    }
}
