package com.github.ares.format.arvo.avro;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.MapType;
import com.github.ares.format.arvo.avro.exception.AresAvroFormatException;
import com.github.ares.format.arvo.avro.exception.AvroFormatErrorCode;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

public class AresRowTypeToAvroSchemaConverter {

    public static Schema buildAvroSchemaWithRowType(AresRowType aresRowType) {
        List<Schema.Field> fields = new ArrayList<>();
        AresDataType<?>[] fieldTypes = aresRowType.getFieldTypes();
        String[] fieldNames = aresRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            fields.add(generateField(fieldNames[i], fieldTypes[i]));
        }
        return Schema.createRecord("AresRecord", null, null, false, fields);
    }

    private static Schema.Field generateField(
            String fieldName, AresDataType<?> aresDataType) {
        return new Schema.Field(
                fieldName,
                aresDataType2AvroDataType(fieldName, aresDataType),
                null,
                null);
    }

    private static Schema aresDataType2AvroDataType(
            String fieldName, AresDataType<?> aresDataType) {

        switch (aresDataType.getSqlType()) {
            case STRING:
                return Schema.create(Schema.Type.STRING);
            case BYTES:
                return Schema.create(Schema.Type.BYTES);
            case TINYINT:
            case SMALLINT:
            case INT:
                return Schema.create(Schema.Type.INT);
            case BIGINT:
                return Schema.create(Schema.Type.LONG);
            case FLOAT:
                return Schema.create(Schema.Type.FLOAT);
            case DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case BOOLEAN:
                return Schema.create(Schema.Type.BOOLEAN);
            case MAP:
                AresDataType<?> valueType = ((MapType<?, ?>) aresDataType).getValueType();
                return Schema.createMap(aresDataType2AvroDataType(fieldName, valueType));
            case ARRAY:
                BasicType<?> elementType = ((ArrayType<?, ?>) aresDataType).getElementType();
                return Schema.createArray(aresDataType2AvroDataType(fieldName, elementType));
            case ROW:
                AresDataType<?>[] fieldTypes =
                        ((AresRowType) aresDataType).getFieldTypes();
                String[] fieldNames = ((AresRowType) aresDataType).getFieldNames();
                List<Schema.Field> subField = new ArrayList<>();
                for (int i = 0; i < fieldNames.length; i++) {
                    subField.add(generateField(fieldNames[i], fieldTypes[i]));
                }
                return Schema.createRecord(fieldName, null, null, false, subField);
            case DECIMAL:
                int precision = ((DecimalType) aresDataType).getPrecision();
                int scale = ((DecimalType) aresDataType).getScale();
                LogicalTypes.Decimal decimal = LogicalTypes.decimal(precision, scale);
                return decimal.addToSchema(Schema.create(Schema.Type.BYTES));
            case TIMESTAMP:
                return LogicalTypes.localTimestampMillis()
                        .addToSchema(Schema.create(Schema.Type.LONG));
            case DATE:
                return LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            case NULL:
                return Schema.create(Schema.Type.NULL);
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
