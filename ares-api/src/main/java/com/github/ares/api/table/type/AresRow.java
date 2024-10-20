package com.github.ares.api.table.type;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class AresRow implements Serializable {
    private static final long serialVersionUID = 1L;
    /** Table identifier. */
    private String tableId = "";
    /** The kind of change that a row describes in a changelog. */
    private RowKind kind = RowKind.INSERT;
    /** The array to store the actual internal format values. */
    private final Object[] fields;

    private volatile int size;

    public AresRow(int arity) {
        this.fields = new Object[arity];
    }

    public AresRow(Object[] fields) {
        this.fields = fields;
    }

    public void setField(int pos, Object value) {
        this.fields[pos] = value;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public void setRowKind(RowKind kind) {
        this.kind = kind;
    }

    public int getArity() {
        return fields.length;
    }

    public String getTableId() {
        return tableId;
    }

    public RowKind getRowKind() {
        return this.kind;
    }

    public Object[] getFields() {
        return fields;
    }

    public Object getField(int pos) {
        return this.fields[pos];
    }

    public AresRow copy() {
        Object[] newFields = new Object[this.getArity()];
        System.arraycopy(this.getFields(), 0, newFields, 0, newFields.length);
        AresRow newRow = new AresRow(newFields);
        newRow.setRowKind(this.getRowKind());
        newRow.setTableId(this.getTableId());
        return newRow;
    }

    public AresRow copy(int[] indexMapping) {
        Object[] newFields = new Object[indexMapping.length];
        for (int i = 0; i < indexMapping.length; i++) {
            newFields[i] = this.fields[indexMapping[i]];
        }
        AresRow newRow = new AresRow(newFields);
        newRow.setRowKind(this.getRowKind());
        newRow.setTableId(this.getTableId());
        return newRow;
    }

    public boolean isNullAt(int pos) {
        return this.fields[pos] == null;
    }

    public int getBytesSize(AresRowType rowType) {
        if (size == 0) {
            int s = 0;
            for (int i = 0; i < fields.length; i++) {
                s += getBytesForValue(fields[i], rowType.getFieldType(i));
            }
            size = s;
        }
        return size;
    }

    /** faster version of {@link #getBytesSize(AresRowType)}. */
    private int getBytesForValue(Object v, AresDataType<?> dataType) {
        if (v == null) {
            return 0;
        }
        SqlType sqlType = dataType.getSqlType();
        switch (sqlType) {
            case STRING:
                return ((String) v).length();
            case BOOLEAN:
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INT:
            case FLOAT:
                return 4;
            case BIGINT:
            case DOUBLE:
                return 8;
            case DECIMAL:
                return 36;
            case NULL:
                return 0;
            case BYTES:
                return ((byte[]) v).length;
            case DATE:
                return 24;
            case TIME:
                return 12;
            case TIMESTAMP:
                return 48;
            case ARRAY:
                return getBytesForArray(v, ((ArrayType) dataType).getElementType());
            case MAP:
                int size = 0;
                MapType<?, ?> mapType = ((MapType<?, ?>) dataType);
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) v).entrySet()) {
                    size +=
                            getBytesForValue(entry.getKey(), mapType.getKeyType())
                                    + getBytesForValue(entry.getValue(), mapType.getValueType());
                }
                return size;
            case ROW:
                int rowSize = 0;
                AresRowType rowType = ((AresRowType) dataType);
                AresDataType<?>[] types = rowType.getFieldTypes();
                AresRow row = (AresRow) v;
                for (int i = 0; i < types.length; i++) {
                    rowSize += getBytesForValue(row.fields[i], types[i]);
                }
                return rowSize;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + sqlType);
        }
    }

    private int getBytesForArray(Object v, BasicType<?> dataType) {
        switch (dataType.getSqlType()) {
            case STRING:
                int s = 0;
                for (String i : ((String[]) v)) {
                    s += i.length();
                }
                return s;
            case BOOLEAN:
                return ((Boolean[]) v).length;
            case TINYINT:
                return ((Byte[]) v).length;
            case SMALLINT:
                return ((Short[]) v).length * 2;
            case INT:
                return ((Integer[]) v).length * 4;
            case FLOAT:
                return ((Float[]) v).length * 4;
            case BIGINT:
                return ((Long[]) v).length * 8;
            case DOUBLE:
                return ((Double[]) v).length * 8;
            case NULL:
            default:
                return 0;
        }
    }

    public int getBytesSize() {
        if (size == 0) {
            int s = 0;
            for (Object field : fields) {
                s += getBytesForValue(field);
            }
            size = s;
        }
        return size;
    }

    private int getBytesForValue(Object v) {
        if (v == null) {
            return 0;
        }
        String clazz = v.getClass().getSimpleName();
        switch (clazz) {
            case "String":
                return ((String) v).length();
            case "Boolean":
            case "Byte":
                return 1;
            case "Short":
                return 2;
            case "Integer":
            case "Float":
                return 4;
            case "Long":
            case "Double":
                return 8;
            case "BigDecimal":
                return 36;
            case "byte[]":
                return ((byte[]) v).length;
            case "LocalDate":
                return 24;
            case "LocalTime":
                return 12;
            case "LocalDateTime":
                return 48;
            case "String[]":
                int s = 0;
                for (String i : ((String[]) v)) {
                    s += i.length();
                }
                return s;
            case "Boolean[]":
                return ((Boolean[]) v).length;
            case "Byte[]":
                return ((Byte[]) v).length;
            case "Short[]":
                return ((Short[]) v).length * 2;
            case "Integer[]":
                return ((Integer[]) v).length * 4;
            case "Long[]":
                return ((Long[]) v).length * 8;
            case "Float[]":
                return ((Float[]) v).length * 4;
            case "Double[]":
                return ((Double[]) v).length * 8;
            case "HashMap":
            case "LinkedHashMap":
                int size = 0;
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) v).entrySet()) {
                    size += getBytesForValue(entry.getKey()) + getBytesForValue(entry.getValue());
                }
                return size;
            case "AresRow":
                int rowSize = 0;
                AresRow row = (AresRow) v;
                for (int i = 0; i < row.fields.length; i++) {
                    rowSize += getBytesForValue(row.fields[i]);
                }
                return rowSize;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + clazz);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AresRow)) {
            return false;
        }
        AresRow that = (AresRow) o;
        return Objects.equals(tableId, that.tableId)
                && kind == that.kind
                && Arrays.deepEquals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(tableId, kind);
        result = 31 * result + Arrays.deepHashCode(fields);
        return result;
    }

    @Override
    public String toString() {
        return "AresRow{"
                + "tableId="
                + tableId
                + ", kind="
                + kind.shortString()
                + ", fields="
                + Arrays.toString(fields)
                + '}';
    }

}