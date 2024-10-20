package com.github.ares.format.text;

import com.github.ares.api.serialization.DeserializationSchema;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.MapType;
import com.github.ares.common.utils.DateTimeUtils;
import com.github.ares.common.utils.DateUtils;
import com.github.ares.common.utils.EncodingUtils;
import com.github.ares.common.utils.TimeUtils;
import com.github.ares.format.text.constant.TextFormatConstant;
import com.github.ares.format.text.exception.AresTextFormatException;
import com.github.ares.format.text.splitor.DefaultTextLineSplitor;
import com.github.ares.format.text.splitor.TextLineSplitor;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TextDeserializationSchema implements DeserializationSchema<AresRow> {
    private final AresRowType aresRowType;
    private final String[] separators;
    private final String encoding;
    private final TextLineSplitor splitor;

    @SuppressWarnings("MagicNumber")
    public static final DateTimeFormatter TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    public Map<String, DateTimeFormatter> fieldFormatterMap = new HashMap<>();

    private TextDeserializationSchema(
            @NonNull AresRowType aresRowType,
            String[] separators,
            String encoding,
            TextLineSplitor splitor) {
        this.aresRowType = aresRowType;
        this.separators = separators;
        this.encoding = encoding;
        this.splitor = splitor;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private AresRowType aresRowType;
        private String[] separators = TextFormatConstant.SEPARATOR.clone();
        private DateUtils.Formatter dateFormatter = DateUtils.Formatter.YYYY_MM_DD;
        private DateTimeUtils.Formatter dateTimeFormatter =
                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
        private TimeUtils.Formatter timeFormatter = TimeUtils.Formatter.HH_MM_SS;
        private String encoding = StandardCharsets.UTF_8.name();
        private TextLineSplitor textLineSplitor = new DefaultTextLineSplitor();

        private Builder() {}

        public Builder aresRowType(AresRowType aresRowType) {
            this.aresRowType = aresRowType;
            return this;
        }

        public Builder delimiter(String delimiter) {
            this.separators[0] = delimiter;
            return this;
        }

        public Builder separators(String[] separators) {
            this.separators = separators;
            return this;
        }

        public Builder dateFormatter(DateUtils.Formatter dateFormatter) {
            this.dateFormatter = dateFormatter;
            return this;
        }

        public Builder dateTimeFormatter(DateTimeUtils.Formatter dateTimeFormatter) {
            this.dateTimeFormatter = dateTimeFormatter;
            return this;
        }

        public Builder timeFormatter(TimeUtils.Formatter timeFormatter) {
            this.timeFormatter = timeFormatter;
            return this;
        }

        public Builder encoding(String encoding) {
            this.encoding = encoding;
            return this;
        }

        public Builder textLineSplitor(TextLineSplitor splitor) {
            this.textLineSplitor = splitor;
            return this;
        }

        public TextDeserializationSchema build() {
            return new TextDeserializationSchema(
                    aresRowType, separators, encoding, textLineSplitor);
        }
    }

    @Override
    public AresRow deserialize(byte[] message) throws IOException {
        String content = new String(message, EncodingUtils.tryParseCharset(encoding));
        Map<Integer, String> splitsMap = splitLineByAresRowType(content, aresRowType, 0);
        Object[] objects = new Object[aresRowType.getTotalFields()];
        for (int i = 0; i < objects.length; i++) {
            objects[i] =
                    convert(
                            splitsMap.get(i),
                            aresRowType.getFieldType(i),
                            0,
                            aresRowType.getFieldNames()[i]);
        }
        return new AresRow(objects);
    }

    @Override
    public AresDataType<AresRow> getProducedType() {
        return aresRowType;
    }

    private Map<Integer, String> splitLineByAresRowType(
            String line, AresRowType aresRowType, int level) {
        String[] splits = splitor.spliteLine(line, separators[level]);
        LinkedHashMap<Integer, String> splitsMap = new LinkedHashMap<>();
        AresDataType<?>[] fieldTypes = aresRowType.getFieldTypes();
        for (int i = 0; i < splits.length; i++) {
            splitsMap.put(i, splits[i]);
        }
        if (fieldTypes.length > splits.length) {
            // contains partition columns
            for (int i = splits.length; i < fieldTypes.length; i++) {
                splitsMap.put(i, null);
            }
        }
        return splitsMap;
    }

    private Object convert(
            String field, AresDataType<?> fieldType, int level, String fieldName) {
        if (StringUtils.isBlank(field)) {
            return null;
        }
        switch (fieldType.getSqlType()) {
            case ARRAY:
                BasicType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                String[] elements = field.split(separators[level + 1]);
                ArrayList<Object> objectArrayList = new ArrayList<>();
                for (String element : elements) {
                    objectArrayList.add(convert(element, elementType, level + 1, fieldName));
                }
                switch (elementType.getSqlType()) {
                    case STRING:
                        return objectArrayList.toArray(new String[0]);
                    case BOOLEAN:
                        return objectArrayList.toArray(new Boolean[0]);
                    case TINYINT:
                        return objectArrayList.toArray(new Byte[0]);
                    case SMALLINT:
                        return objectArrayList.toArray(new Short[0]);
                    case INT:
                        return objectArrayList.toArray(new Integer[0]);
                    case BIGINT:
                        return objectArrayList.toArray(new Long[0]);
                    case FLOAT:
                        return objectArrayList.toArray(new Float[0]);
                    case DOUBLE:
                        return objectArrayList.toArray(new Double[0]);
                    default:
                        throw new AresTextFormatException(
                                String.format(
                                        "Ares array not support this data type [%s]",
                                        elementType.getSqlType()));
                }
            case MAP:
                AresDataType<?> keyType = ((MapType<?, ?>) fieldType).getKeyType();
                AresDataType<?> valueType = ((MapType<?, ?>) fieldType).getValueType();
                LinkedHashMap<Object, Object> objectMap = new LinkedHashMap<>();
                String[] kvs = field.split(separators[level + 1]);
                for (String kv : kvs) {
                    String[] splits = kv.split(separators[level + 2]);
                    if (splits.length < 2) {
                        objectMap.put(convert(splits[0], keyType, level + 1, fieldName), null);
                    } else {
                        objectMap.put(
                                convert(splits[0], keyType, level + 1, fieldName),
                                convert(splits[1], valueType, level + 1, fieldName));
                    }
                }
                return objectMap;
            case STRING:
                return field;
            case BOOLEAN:
                return Boolean.parseBoolean(field);
            case TINYINT:
                return Byte.parseByte(field);
            case SMALLINT:
                return Short.parseShort(field);
            case INT:
                return Integer.parseInt(field);
            case BIGINT:
                return Long.parseLong(field);
            case FLOAT:
                return Float.parseFloat(field);
            case DOUBLE:
                return Double.parseDouble(field);
            case DECIMAL:
                return new BigDecimal(field);
            case NULL:
                return null;
            case BYTES:
                return field.getBytes(StandardCharsets.UTF_8);
            case DATE:
                DateTimeFormatter dateFormatter = fieldFormatterMap.get(fieldName);
                if (dateFormatter == null) {
                    dateFormatter = DateUtils.matchDateFormatter(field);
                    fieldFormatterMap.put(fieldName, dateFormatter);
                }

                return dateFormatter.parse(field).query(TemporalQueries.localDate());
            case TIME:
                TemporalAccessor parsedTime = TIME_FORMAT.parse(field);
                return parsedTime.query(TemporalQueries.localTime());
            case TIMESTAMP:
                DateTimeFormatter dateTimeFormatter = fieldFormatterMap.get(fieldName);
                if (dateTimeFormatter == null) {
                    dateTimeFormatter = DateTimeUtils.matchDateTimeFormatter(field);
                    fieldFormatterMap.put(fieldName, dateTimeFormatter);
                }

                TemporalAccessor parsedTimestamp = dateTimeFormatter.parse(field);
                LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
                LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
                return LocalDateTime.of(localDate, localTime);
            case ROW:
                Map<Integer, String> splitsMap =
                        splitLineByAresRowType(field, (AresRowType) fieldType, level + 1);
                Object[] objects = new Object[splitsMap.size()];
                String[] eleFieldNames = ((AresRowType) fieldType).getFieldNames();
                for (int i = 0; i < objects.length; i++) {
                    objects[i] =
                            convert(
                                    splitsMap.get(i),
                                    ((AresRowType) fieldType).getFieldType(i),
                                    level + 1,
                                    fieldName + "." + eleFieldNames[i]);
                }
                return new AresRow(objects);
            default:
                throw new AresTextFormatException(
                        String.format(
                                "Ares not support this data type [%s]",
                                fieldType.getSqlType()));
        }
    }
}
