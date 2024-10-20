package com.github.ares.format.arvo.avro;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.format.arvo.avro.exception.AresAvroFormatException;
import com.github.ares.format.arvo.avro.exception.AvroFormatErrorCode;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class RowToAvroConverter implements Serializable {

    private static final long serialVersionUID = -576124379280229724L;

    private final Schema schema;
    private final AresRowType rowType;
    private final DatumWriter<GenericRecord> writer;

    public RowToAvroConverter(AresRowType rowType) {
        this.schema = AresRowTypeToAvroSchemaConverter.buildAvroSchemaWithRowType(rowType);
        this.rowType = rowType;
        this.writer = createWriter();
    }

    private DatumWriter<GenericRecord> createWriter() {
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        datumWriter.getData().addLogicalTypeConversion(new Conversions.DecimalConversion());
        datumWriter.getData().addLogicalTypeConversion(new TimeConversions.DateConversion());
        datumWriter
                .getData()
                .addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        return datumWriter;
    }

    public Schema getSchema() {
        return schema;
    }

    public DatumWriter<GenericRecord> getWriter() {
        return writer;
    }

    public GenericRecord convertRowToGenericRecord(AresRow element) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        String[] fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = rowType.getFieldName(i);
            Object value = element.getField(i);
            builder.set(fieldName.toLowerCase(), resolveObject(value, rowType.getFieldType(i)));
        }
        return builder.build();
    }

    private Object resolveObject(Object data, AresDataType<?> aresDataType) {
        if (data == null) {
            return null;
        }
        switch (aresDataType.getSqlType()) {
            case STRING:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case MAP:
            case DECIMAL:
            case DATE:
            case TIMESTAMP:
                return data;
            case TINYINT:
                Class<?> typeClass = aresDataType.getTypeClass();
                if (typeClass == Byte.class) {
                    if (data instanceof Byte) {
                        Byte aByte = (Byte) data;
                        return Byte.toUnsignedInt(aByte);
                    }
                }
                return data;
            case BYTES:
                return ByteBuffer.wrap((byte[]) data);
            case ARRAY:
                BasicType<?> basicType = ((ArrayType<?, ?>) aresDataType).getElementType();
                int length = Array.getLength(data);
                ArrayList<Object> records = new ArrayList<>(length);
                for (int i = 0; i < length; i++) {
                    records.add(resolveObject(Array.get(data, i), basicType));
                }
                return records;
            case ROW:
                AresRow aresRow = (AresRow) data;
                AresDataType<?>[] fieldTypes =
                        ((AresRowType) aresDataType).getFieldTypes();
                String[] fieldNames = ((AresRowType) aresDataType).getFieldNames();
                Schema recordSchema =
                        AresRowTypeToAvroSchemaConverter.buildAvroSchemaWithRowType(
                                (AresRowType) aresDataType);
                GenericRecordBuilder recordBuilder = new GenericRecordBuilder(recordSchema);
                for (int i = 0; i < fieldNames.length; i++) {
                    recordBuilder.set(
                            fieldNames[i].toLowerCase(),
                            resolveObject(aresRow.getField(i), fieldTypes[i]));
                }
                return recordBuilder.build();
            default:
                String errorMsg =
                        String.format(
                                "Ares avro format is not supported for this data type [%s]",
                                aresDataType.getSqlType());
                throw new AresAvroFormatException(
                        AvroFormatErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }
}
