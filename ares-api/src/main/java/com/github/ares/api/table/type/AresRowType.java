package com.github.ares.api.table.type;

import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;

@Getter
public class AresRowType implements CompositeType<AresRow> {
    private static final long serialVersionUID = 1L;

    /**
     * The field name of the {@link AresRow}.
     */
    @Setter
    private String[] fieldNames;
    /**
     * The type of the field.
     */
    private final AresDataType<?>[] fieldTypes;

    @Setter
    private String[] targetFieldNames;

    @Setter
    private AresDataType<?>[] targetFieldTypes;

    public AresRowType(String[] fieldNames, AresDataType<?>[] fieldTypes) {
        if (fieldNames.length != fieldTypes.length) {
            throw new RuntimeException("The number of field names must be the same as the number of field types.");
        }
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public Class<AresRow> getTypeClass() {
        return AresRow.class;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.ROW;
    }


    @Override
    public List<AresDataType<?>> getChildren() {
        return Arrays.asList(fieldTypes);
    }

    public int getTotalFields() {
        return fieldTypes.length;
    }

    public String getFieldName(int index) {
        return fieldNames[index];
    }

    public AresDataType<?> getFieldType(int index) {
        return fieldTypes[index];
    }

    public int indexOf(String fieldName) {
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(fieldName)) {
                return i;
            }
        }
        throw new IllegalArgumentException(String.format("can't find field [%s]", fieldName));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AresRowType)) {
            return false;
        }
        AresRowType that = (AresRowType) obj;
        return Arrays.equals(fieldNames, that.fieldNames)
                && Arrays.equals(fieldTypes, that.fieldTypes);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(fieldNames);
        result = 31 * result + Arrays.hashCode(fieldTypes);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("ROW<");
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(fieldNames[i]).append(" ").append(fieldTypes[i]);
        }
        return builder.append(">").toString();
    }
}
