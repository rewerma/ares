package com.github.ares.format.arvo.avro;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.MapType;
import com.github.ares.format.arvo.avro.exception.AresAvroFormatException;
import com.github.ares.format.arvo.avro.exception.AvroFormatErrorCode;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroToRowConverter implements Serializable {

    private static final long serialVersionUID = 8177020083886379563L;

    private DatumReader<GenericRecord> reader = null;
    private Schema schema;

    public AvroToRowConverter(AresRowType rowType) {
        schema = AresRowTypeToAvroSchemaConverter.buildAvroSchemaWithRowType(rowType);
    }

    public DatumReader<GenericRecord> getReader() {
        if (reader == null) {
            reader = createReader();
        }
        return reader;
    }

    private DatumReader<GenericRecord> createReader() {
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema, schema);
        datumReader.getData().addLogicalTypeConversion(new Conversions.DecimalConversion());
        datumReader.getData().addLogicalTypeConversion(new TimeConversions.DateConversion());
        datumReader
                .getData()
                .addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        return datumReader;
    }

    public AresRow converter(GenericRecord record, AresRowType rowType) {
        String[] fieldNames = rowType.getFieldNames();

        Object[] values = new Object[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            if (record.getSchema().getField(fieldNames[i]) == null) {
                values[i] = null;
                continue;
            }
            values[i] = convertField(rowType.getFieldType(i), record.get(fieldNames[i]));
        }
        return new AresRow(values);
    }

    private Object convertField(AresDataType<?> dataType, Object val) {
        switch (dataType.getSqlType()) {
            case STRING:
                return val.toString();
            case BOOLEAN:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case NULL:
            case DATE:
            case DECIMAL:
            case TIMESTAMP:
                return val;
            case BYTES:
                return ((ByteBuffer) val).array();
            case SMALLINT:
                return ((Integer) val).shortValue();
            case TINYINT:
                Class<?> typeClass = dataType.getTypeClass();
                if (typeClass == Byte.class) {
                    Integer integer = (Integer) val;
                    return integer.byteValue();
                }
                return val;
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) dataType;
                Map<Object, Object> res = new HashMap<>();
                Map map = (Map) val;
                for (Object o : map.entrySet()) {
                    res.put(
                            convertField(mapType.getKeyType(), ((Map.Entry) o).getKey()),
                            convertField(mapType.getValueType(), ((Map.Entry) o).getValue()));
                }
                return res;
            case ARRAY:
                BasicType<?> basicType = ((ArrayType<?, ?>) dataType).getElementType();
                List<Object> list = (List<Object>) val;
                return convertArray(list, basicType);
            case ROW:
                AresRowType subRow = (AresRowType) dataType;
                return converter((GenericRecord) val, subRow);
            default:
                String errorMsg =
                        String.format(
                                "Ares avro format is not supported for this data type [%s]",
                                dataType.getSqlType());
                throw new AresAvroFormatException(
                        AvroFormatErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    protected Object convertArray(List<Object> val, AresDataType<?> dataType) {
        if (val == null) {
            return null;
        }
        int length = val.size();
        Object instance = Array.newInstance(dataType.getTypeClass(), length);
        for (int i = 0; i < val.size(); i++) {
            Array.set(instance, i, convertField(dataType, val.get(i)));
        }
        return instance;
    }
}
