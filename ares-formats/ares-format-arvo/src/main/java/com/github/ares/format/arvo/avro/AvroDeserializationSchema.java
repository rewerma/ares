package com.github.ares.format.arvo.avro;

import com.github.ares.api.serialization.DeserializationSchema;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;

public class AvroDeserializationSchema implements DeserializationSchema<AresRow> {

    private static final long serialVersionUID = -7907358485475741366L;

    private final AresRowType rowType;
    private final AvroToRowConverter converter;

    public AvroDeserializationSchema(AresRowType rowType) {
        this.rowType = rowType;
        this.converter = new AvroToRowConverter(rowType);
    }

    @Override
    public AresRow deserialize(byte[] message) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(message, null);
        GenericRecord record = this.converter.getReader().read(null, decoder);
        return converter.converter(record, rowType);
    }

    @Override
    public AresDataType<AresRow> getProducedType() {
        return this.rowType;
    }
}
