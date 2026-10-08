package com.github.ares.connector.doris.febe;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.common.exceptions.AresException;
import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;

final class ArrowBatchReader {
    private ArrowBatchReader() {}

    static List<AresRow> read(byte[] bytes, AresRowType rowType, BufferAllocator allocator) {
        if (bytes == null || bytes.length == 0) {
            return new ArrayList<>();
        }
        try (ArrowStreamReader reader =
                new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
            List<AresRow> rows = new ArrayList<>();
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                FieldVector[] vectors = resolveVectors(root, rowType);
                for (int rowIndex = 0; rowIndex < root.getRowCount(); rowIndex++) {
                    AresRow row = new AresRow(rowType.getTotalFields());
                    for (int column = 0; column < rowType.getTotalFields(); column++) {
                        row.setField(
                                column,
                                readField(vectors[column], rowIndex, rowType.getFieldType(column)));
                    }
                    rows.add(row);
                }
            }
            return rows;
        } catch (AresException e) {
            throw e;
        } catch (Exception e) {
            throw new AresException("Failed to read Doris Arrow batch", e);
        }
    }

    private static FieldVector[] resolveVectors(VectorSchemaRoot root, AresRowType rowType) {
        FieldVector[] vectors = new FieldVector[rowType.getTotalFields()];
        List<Field> fields = root.getSchema().getFields();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String name = rowType.getFieldName(i);
            FieldVector matched = null;
            for (int arrowIndex = 0; arrowIndex < fields.size(); arrowIndex++) {
                if (fields.get(arrowIndex).getName().equalsIgnoreCase(name)) {
                    matched = root.getVector(arrowIndex);
                    break;
                }
            }
            if (matched == null && i < root.getFieldVectors().size()) {
                matched = root.getVector(i);
            }
            vectors[i] = matched;
        }
        return vectors;
    }

    private static Object readField(FieldVector vector, int rowIndex, AresDataType<?> targetType) {
        if (vector == null || vector.isNull(rowIndex)) {
            return null;
        }
        return coerce(readRaw(vector, rowIndex), targetType);
    }

    private static Object readRaw(FieldVector vector, int rowIndex) {
        if (vector instanceof BitVector) {
            return ((BitVector) vector).get(rowIndex) == 1;
        }
        if (vector instanceof TinyIntVector) {
            return (int) ((TinyIntVector) vector).get(rowIndex);
        }
        if (vector instanceof SmallIntVector) {
            return (int) ((SmallIntVector) vector).get(rowIndex);
        }
        if (vector instanceof IntVector) {
            return ((IntVector) vector).get(rowIndex);
        }
        if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(rowIndex);
        }
        if (vector instanceof Float4Vector) {
            return ((Float4Vector) vector).get(rowIndex);
        }
        if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(rowIndex);
        }
        if (vector instanceof DecimalVector) {
            return ((DecimalVector) vector).getObject(rowIndex);
        }
        if (vector instanceof VarCharVector) {
            return new String(((VarCharVector) vector).get(rowIndex), StandardCharsets.UTF_8);
        }
        if (vector instanceof VarBinaryVector) {
            return ((VarBinaryVector) vector).get(rowIndex);
        }
        if (vector instanceof DateDayVector) {
            return LocalDate.ofEpochDay(((DateDayVector) vector).get(rowIndex));
        }
        if (vector instanceof TimeStampMicroVector) {
            return fromEpochMicros(((TimeStampMicroVector) vector).get(rowIndex));
        }
        if (vector instanceof TimeStampMicroTZVector) {
            return fromEpochMicros(((TimeStampMicroTZVector) vector).get(rowIndex));
        }
        if (vector instanceof TimeStampMilliVector) {
            return fromEpochMillis(((TimeStampMilliVector) vector).get(rowIndex));
        }
        if (vector instanceof TimeStampMilliTZVector) {
            return fromEpochMillis(((TimeStampMilliTZVector) vector).get(rowIndex));
        }
        if (vector instanceof TimeStampNanoVector) {
            return fromEpochNanos(((TimeStampNanoVector) vector).get(rowIndex));
        }
        if (vector instanceof TimeStampNanoTZVector) {
            return fromEpochNanos(((TimeStampNanoTZVector) vector).get(rowIndex));
        }
        if (vector instanceof TimeStampSecVector) {
            return LocalDateTime.ofEpochSecond(
                    ((TimeStampSecVector) vector).get(rowIndex), 0, ZoneOffset.UTC);
        }
        if (vector instanceof TimeStampSecTZVector) {
            return LocalDateTime.ofEpochSecond(
                    ((TimeStampSecTZVector) vector).get(rowIndex), 0, ZoneOffset.UTC);
        }
        if (vector instanceof TimeMicroVector) {
            return LocalTime.ofNanoOfDay(((TimeMicroVector) vector).get(rowIndex) * 1000L);
        }
        if (vector instanceof TimeMilliVector) {
            return LocalTime.ofNanoOfDay(((TimeMilliVector) vector).get(rowIndex) * 1_000_000L);
        }
        if (vector instanceof TimeNanoVector) {
            return LocalTime.ofNanoOfDay(((TimeNanoVector) vector).get(rowIndex));
        }
        if (vector instanceof TimeSecVector) {
            return LocalTime.ofSecondOfDay(((TimeSecVector) vector).get(rowIndex));
        }
        Object value = vector.getObject(rowIndex);
        return value == null ? null : String.valueOf(value);
    }

    private static Object coerce(Object value, AresDataType<?> targetType) {
        if (value == null || targetType == null) {
            return value;
        }
        switch (targetType.getSqlType()) {
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                if (value instanceof Number) {
                    return ((Number) value).intValue() != 0;
                }
                return Boolean.valueOf(String.valueOf(value));
            case TINYINT:
            case SMALLINT:
            case INT:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return Integer.valueOf(String.valueOf(value));
            case BIGINT:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                return Long.valueOf(String.valueOf(value));
            case FLOAT:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                return Float.valueOf(String.valueOf(value));
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                return Double.valueOf(String.valueOf(value));
            case DECIMAL:
                if (value instanceof BigDecimal) {
                    return value;
                }
                if (value instanceof Number) {
                    return BigDecimal.valueOf(((Number) value).doubleValue());
                }
                DecimalType decimalType = (DecimalType) targetType;
                return new BigDecimal(String.valueOf(value))
                        .setScale(decimalType.getScale(), BigDecimal.ROUND_HALF_UP);
            case STRING:
                if (value instanceof byte[]) {
                    return new String((byte[]) value, StandardCharsets.UTF_8);
                }
                return String.valueOf(value);
            case BYTES:
                if (value instanceof byte[]) {
                    return value;
                }
                return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
            case DATE:
                if (value instanceof LocalDate) {
                    return value;
                }
                if (value instanceof LocalDateTime) {
                    return ((LocalDateTime) value).toLocalDate();
                }
                return LocalDate.parse(String.valueOf(value).substring(0, 10));
            case TIME:
                if (value instanceof LocalTime) {
                    return value;
                }
                if (value instanceof LocalDateTime) {
                    return ((LocalDateTime) value).toLocalTime();
                }
                return LocalTime.parse(String.valueOf(value));
            case TIMESTAMP:
                if (value instanceof LocalDateTime) {
                    return value;
                }
                if (value instanceof LocalDate) {
                    return ((LocalDate) value).atStartOfDay();
                }
                return LocalDateTime.parse(String.valueOf(value).replace(' ', 'T'));
            default:
                return value;
        }
    }

    private static LocalDateTime fromEpochMicros(long micros) {
        long seconds = Math.floorDiv(micros, 1_000_000L);
        int nanos = (int) Math.floorMod(micros, 1_000_000L) * 1000;
        return LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC);
    }

    private static LocalDateTime fromEpochMillis(long millis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }

    private static LocalDateTime fromEpochNanos(long nanos) {
        long seconds = Math.floorDiv(nanos, 1_000_000_000L);
        int nanoOfSecond = (int) Math.floorMod(nanos, 1_000_000_000L);
        return LocalDateTime.ofEpochSecond(seconds, nanoOfSecond, ZoneOffset.UTC);
    }
}
