package com.github.ares.format.arvo.avro;

import com.github.ares.api.serialization.SerializationSchema;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.format.arvo.avro.exception.AresAvroFormatException;
import com.github.ares.format.arvo.avro.exception.AvroFormatErrorCode;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroSerializationSchema implements SerializationSchema {

    private static final long serialVersionUID = 4438784443025715370L;

    private final ByteArrayOutputStream out;
    private final BinaryEncoder encoder;
    private final RowToAvroConverter converter;
    private final DatumWriter<GenericRecord> writer;

    public AvroSerializationSchema(AresRowType rowType) {
        this.out = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(out, null);
        this.converter = new RowToAvroConverter(rowType);
        this.writer = this.converter.getWriter();
    }

    @Override
    public byte[] serialize(AresRow element) {
        GenericRecord record = converter.convertRowToGenericRecord(element);
        try {
            writer.write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new AresAvroFormatException(
                    AvroFormatErrorCode.SERIALIZATION_ERROR,
                    "Serialization error on record : " + element);
        } finally {
            out.reset();
        }
    }
}
