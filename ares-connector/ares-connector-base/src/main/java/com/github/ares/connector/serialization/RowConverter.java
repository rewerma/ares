package com.github.ares.connector.serialization;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.MapType;
import com.github.ares.api.table.type.SqlType;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Conversion between {@link AresRow} & engine's row.
 *
 * @param <T> engine row
 */
public abstract class RowConverter<T> implements Serializable {
    protected final AresDataType<?> dataType;

    protected RowConverter(AresDataType<?> dataType) {
        this.dataType = dataType;
    }

    public void validate(AresRow aresRow) {
        if (!(dataType instanceof AresRowType)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The data type don't support validation: %s. ",
                            dataType.getClass().getSimpleName()));
        }
        AresDataType<?>[] fieldTypes = ((AresRowType) dataType).getFieldTypes();
        List<String> errors = new ArrayList<>();
        Object field;
        AresDataType<?> fieldType;
        for (int i = 0; i < fieldTypes.length; i++) {
            field = aresRow.getField(i);
            fieldType = fieldTypes[i];
            if (!validate(field, fieldType)) {
                errors.add(
                        String.format(
                                "The SQL type '%s' don't support '%s', the class of the expected data type is '%s'.",
                                fieldType.getSqlType(),
                                field.getClass(),
                                fieldType.getTypeClass()));
            }
        }
        if (!errors.isEmpty()) {
            throw new UnsupportedOperationException(String.join(",", errors));
        }
    }

    protected boolean validate(Object field, AresDataType<?> dataType) {
        if (field == null || dataType.getSqlType() == SqlType.NULL) {
            return true;
        }
        SqlType sqlType = dataType.getSqlType();
        if (field instanceof Boolean && (sqlType == SqlType.BOOLEAN || sqlType == SqlType.INT)) {
            return true;
        }
        switch (sqlType) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case DECIMAL:
            case BYTES:
                return dataType.getTypeClass() == field.getClass();
            case ARRAY:
                if (!(field instanceof Object[])) {
                    return false;
                }
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) dataType;
                Object[] arrayField = (Object[]) field;
                if (arrayField.length == 0) {
                    return true;
                } else {
                    return validate(arrayField[0], arrayType.getElementType());
                }
            case MAP:
                if (!(field instanceof Map)) {
                    return false;
                }
                MapType<?, ?> mapType = (MapType<?, ?>) dataType;
                Map<?, ?> mapField = (Map<?, ?>) field;
                if (mapField.size() == 0) {
                    return true;
                } else {
                    Map.Entry<?, ?> entry = mapField.entrySet().stream().findFirst().get();
                    Object key = entry.getKey();
                    Object value = entry.getValue();
                    /* if (key instanceof scala.Some) {
                        key = ((scala.Some<?>) key).get();
                    } if (value instanceof scala.Some) {
                        value = ((scala.Some<?>) value).get();
                    }*/
                    return validate(key, mapType.getKeyType())
                            && validate(value, mapType.getValueType());
                }
            case ROW:
                if (!(field instanceof AresRow)) {
                    return false;
                }
                AresDataType<?>[] fieldTypes = ((AresRowType) dataType).getFieldTypes();
                AresRow aresRow = (AresRow) field;
                for (int i = 0; i < fieldTypes.length; i++) {
                    if (!validate(aresRow.getField(i), fieldTypes[i])) {
                        return false;
                    }
                }
                return true;
            default:
                return false;
        }
    }

    /**
     * Convert {@link AresRow} to engine's row.
     *
     * @throws IOException Thrown, if the conversion fails.
     */
    public abstract T convert(AresRow aresRow) throws IOException;

    /**
     * Convert engine's row to {@link AresRow}.
     *
     * @throws IOException Thrown, if the conversion fails.
     */
    public abstract AresRow reconvert(T engineRow) throws IOException;
}
