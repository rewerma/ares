package com.github.ares.format.text;

import com.github.ares.api.serialization.SerializationSchema;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.MapType;
import com.github.ares.common.utils.DateTimeUtils;
import com.github.ares.common.utils.DateUtils;
import com.github.ares.common.utils.TimeUtils;
import com.github.ares.format.text.constant.TextFormatConstant;
import com.github.ares.format.text.exception.AresTextFormatException;
import lombok.NonNull;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class TextSerializationSchema implements SerializationSchema {
    private final AresRowType aresRowType;
    private final String[] separators;
    private final DateUtils.Formatter dateFormatter;
    private final DateTimeUtils.Formatter dateTimeFormatter;
    private final TimeUtils.Formatter timeFormatter;
    private final Charset charset;

    private TextSerializationSchema(
            @NonNull AresRowType aresRowType,
            String[] separators,
            DateUtils.Formatter dateFormatter,
            DateTimeUtils.Formatter dateTimeFormatter,
            TimeUtils.Formatter timeFormatter,
            Charset charset) {
        this.aresRowType = aresRowType;
        this.separators = separators;
        this.dateFormatter = dateFormatter;
        this.dateTimeFormatter = dateTimeFormatter;
        this.timeFormatter = timeFormatter;
        this.charset = charset;
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
        private Charset charset = StandardCharsets.UTF_8;

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

        public Builder charset(Charset charset) {
            this.charset = charset;
            return this;
        }

        public TextSerializationSchema build() {
            return new TextSerializationSchema(
                    aresRowType,
                    separators,
                    dateFormatter,
                    dateTimeFormatter,
                    timeFormatter,
                    charset);
        }
    }

    @Override
    public byte[] serialize(AresRow element) {
        if (element.getFields().length != aresRowType.getTotalFields()) {
            throw new IndexOutOfBoundsException(
                    "The data does not match the configured schema information, please check");
        }
        Object[] fields = element.getFields();
        String[] strings = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            strings[i] = convert(fields[i], aresRowType.getFieldType(i), 0);
        }
        return String.join(separators[0], strings).getBytes(charset);
    }

    private String convert(Object field, AresDataType<?> fieldType, int level) {
        if (field == null) {
            return "";
        }
        switch (fieldType.getSqlType()) {
            case DOUBLE:
            case FLOAT:
            case INT:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case BIGINT:
            case DECIMAL:
                return field.toString();
            case STRING:
                byte[] bytes = field.toString().getBytes(StandardCharsets.UTF_8);
                return new String(bytes, StandardCharsets.UTF_8);
            case DATE:
                return DateUtils.toString((LocalDate) field, dateFormatter);
            case TIME:
                return TimeUtils.toString((LocalTime) field, timeFormatter);
            case TIMESTAMP:
                return DateTimeUtils.toString((LocalDateTime) field, dateTimeFormatter);
            case NULL:
                return "";
            case BYTES:
                return new String((byte[]) field, StandardCharsets.UTF_8);
            case ARRAY:
                BasicType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                return Arrays.stream((Object[]) field)
                        .map(f -> convert(f, elementType, level + 1))
                        .collect(Collectors.joining(separators[level + 1]));
            case MAP:
                AresDataType<?> keyType = ((MapType<?, ?>) fieldType).getKeyType();
                AresDataType<?> valueType = ((MapType<?, ?>) fieldType).getValueType();
                return ((Map<Object, Object>) field)
                        .entrySet().stream()
                                .map(
                                        entry ->
                                                String.join(
                                                        separators[level + 2],
                                                        convert(entry.getKey(), keyType, level + 1),
                                                        convert(
                                                                entry.getValue(),
                                                                valueType,
                                                                level + 1)))
                                .collect(Collectors.joining(separators[level + 1]));
            case ROW:
                Object[] fields = ((AresRow) field).getFields();
                String[] strings = new String[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    strings[i] =
                            convert(
                                    fields[i],
                                    ((AresRowType) fieldType).getFieldType(i),
                                    level + 1);
                }
                return String.join(separators[level + 1], strings);
            default:
                throw new AresTextFormatException(
                        String.format(
                                "Ares format text not supported for parsing this type [%s]",
                                fieldType.getSqlType()));
        }
    }
}
