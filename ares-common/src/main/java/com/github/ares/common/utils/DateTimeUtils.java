package com.github.ares.common.utils;

import com.github.ares.common.exceptions.AresException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class DateTimeUtils {
    private static final Map<Formatter, DateTimeFormatter> FORMATTER_MAP =
            new HashMap<Formatter, DateTimeFormatter>();

    static {
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SPOT,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SPOT.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SLASH,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SLASH.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_NO_SPLIT,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_NO_SPLIT.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_ISO8601,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_ISO8601.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SSS_ISO8601,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SSS_ISO8601.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS_ISO8601,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSS_ISO8601.value));
        FORMATTER_MAP.put(
                Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSSSSS_ISO8601,
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SSSSSSSSS_ISO8601.value));
    }

    // if the datatime string length is 19, find the DateTimeFormatter from this map
    public static final Map<Pattern, DateTimeFormatter> YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP =
            new LinkedHashMap<>();
    public static Set<Map.Entry<Pattern, DateTimeFormatter>>
            YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP_ENTRY_SET = new LinkedHashSet<>();

    // if the datatime string length bigger than 19, find the DateTimeFormatter from this map
    public static final Map<Pattern, DateTimeFormatter> YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP =
            new LinkedHashMap<>();
    public static Set<Map.Entry<Pattern, DateTimeFormatter>>
            YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP_ENTRY_SET = new LinkedHashSet<>();

    // if the datatime string length is 14, use this formatter
    public static final DateTimeFormatter YYYY_MM_DD_HH_MM_SS_14_FORMATTER =
            DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_NO_SPLIT.value);

    static {
        YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}"),
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS.value));

        YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}.*"),
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(DateTimeFormatter.ISO_LOCAL_DATE)
                        .appendLiteral(' ')
                        .append(DateTimeFormatter.ISO_LOCAL_TIME)
                        .toFormatter());

        YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}"),
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_ISO8601.value));

        YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}.*"),
                DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}:\\d{2}"),
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SLASH.value));

        YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}.*"),
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(
                                new DateTimeFormatterBuilder()
                                        .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                                        .appendLiteral('/')
                                        .appendValue(MONTH_OF_YEAR, 2)
                                        .appendLiteral('/')
                                        .appendValue(DAY_OF_MONTH, 2)
                                        .toFormatter())
                        .appendLiteral(' ')
                        .append(DateTimeFormatter.ISO_LOCAL_TIME)
                        .toFormatter());

        YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}:\\d{2}"),
                DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SPOT.value));

        YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}.*"),
                new DateTimeFormatterBuilder()
                        .parseCaseInsensitive()
                        .append(
                                new DateTimeFormatterBuilder()
                                        .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                                        .appendLiteral('.')
                                        .appendValue(MONTH_OF_YEAR, 2)
                                        .appendLiteral('.')
                                        .appendValue(DAY_OF_MONTH, 2)
                                        .toFormatter())
                        .appendLiteral(' ')
                        .append(DateTimeFormatter.ISO_LOCAL_TIME)
                        .toFormatter());

        YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP.put(
                Pattern.compile("\\d{4}年\\d{2}月\\d{2}日\\s\\d{2}时\\d{2}分\\d{2}秒"),
                DateTimeFormatter.ofPattern("yyyy年MM月dd日 HH时mm分ss秒"));

        YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP_ENTRY_SET.addAll(
                YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP.entrySet());
        YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP_ENTRY_SET.addAll(
                YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP.entrySet());
    }

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter DATE_TIME_FORMATTER_WITH_MILLIS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
    public static final DateTimeFormatter DATE_TIME_FORMATTER_WITH_MILLIS2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS");
    public static final DateTimeFormatter DATE_TIME_FORMATTER_WITH_MILLIS3 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    public static final DateTimeFormatter DATE_TIME_FORMATTER_WITH_MILLIS4 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS");
    public static final DateTimeFormatter DATE_TIME_FORMATTER_WITH_MILLIS5 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS");
    public static final DateTimeFormatter DATE_TIME_FORMATTER_WITH_MILLIS6 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    public static String localDateTimeToString(LocalDateTime localDateTime) {
        long nano = localDateTime.getNano();
        int nanoLength = 0;
        if (nano != 0) {
            String nanoStr = String.valueOf(nano);
            byte[] bytes = nanoStr.getBytes();
            int zerosLength = 0;
            for (int i = bytes.length - 1; i >= 0; i--) {
                if (bytes[i] == (byte) '0') {
                    zerosLength += 1;
                } else {
                    break;
                }
            }
            nanoLength = 9 - zerosLength;
        }
        switch (nanoLength) {
            case 0:
                return localDateTime.format(DATE_TIME_FORMATTER);
            case 1:
                return localDateTime.format(DATE_TIME_FORMATTER_WITH_MILLIS);
            case 2:
                return localDateTime.format(DATE_TIME_FORMATTER_WITH_MILLIS2);
            case 3:
                return localDateTime.format(DATE_TIME_FORMATTER_WITH_MILLIS3);
            case 4:
                return localDateTime.format(DATE_TIME_FORMATTER_WITH_MILLIS4);
            case 5:
                return localDateTime.format(DATE_TIME_FORMATTER_WITH_MILLIS5);
            default:
            case 6:
                return localDateTime.format(DATE_TIME_FORMATTER_WITH_MILLIS6);
        }
    }

    public static LocalDateTime stringToLocalDateTime(String s) {
        return stringToLocalDateTime(s, ZoneId.systemDefault());
    }

    @SuppressWarnings("unchecked")
    public static LocalDateTime stringToLocalDateTime(String s, ZoneId timeZoneId) {
        try {
            Object[] parsedString = parseTimestampString(s);
            List<Integer> segments = (List<Integer>) parsedString[0];
            ZoneId parsedZoneId = (ZoneId) parsedString[1];
            boolean justTime = (boolean) parsedString[2];
            if (segments.isEmpty()) {
                return null;
            }
            ZoneId zoneId = timeZoneId;
            if (parsedZoneId != null) {
                zoneId = parsedZoneId;
            }
            long nanoseconds = MICROSECONDS.toNanos(segments.get(6));
            LocalTime localTime = LocalTime.of(segments.get(3), segments.get(4), segments.get(5), (int) nanoseconds);
            LocalDate localDate;
            if (justTime) {
                localDate = LocalDate.now(zoneId);
            } else {
                localDate = LocalDate.of(segments.get(0), segments.get(1), segments.get(2));
            }
            return LocalDateTime.of(localDate, localTime);
        } catch (Exception e) {
            throw new AresException("Failed to parse timestamp string: " + s);
        }
    }

    private static Object[] parseTimestampString(String s) {
        Object[] result = new Object[3];
        Object[] emptyResult = new Object[]{new ArrayList<>(), null, false};
        if (s == null || s.trim().isEmpty()) {
            return emptyResult;
        }
        String tz = null;
        List<Integer> segments = new ArrayList<>(Arrays.asList(1, 1, 1, 0, 0, 0, 0, 0, 0));
        int i = 0;
        int currentSegmentValue = 0;
        int currentSegmentDigits = 0;
        byte[] bytes = s.trim().getBytes();
        int j = 0;
        int digitsMilli = 0;
        boolean justTime = false;
        Integer yearSign = null;
        if (bytes[j] == '-' || bytes[j] == '+') {
            if (bytes[j] == '-') yearSign = -1;
            else yearSign = 1;
            j += 1;
        }
        while (j < bytes.length) {
            byte b = bytes[j];
            int parsedValue = b - (byte) '0';
            if (parsedValue < 0 || parsedValue > 9) {
                if (j == 0 && b == 'T') {
                    justTime = true;
                    i += 3;
                } else if (i < 2) {
                    if (b == '-') {
                        if (!isValidDigits(i, currentSegmentDigits)) {
                            return emptyResult;
                        }
                        segments.set(i, currentSegmentValue);
                        currentSegmentValue = 0;
                        currentSegmentDigits = 0;
                        i += 1;
                    } else if (i == 0 && b == ':' && yearSign == null) {
                        justTime = true;
                        if (!isValidDigits(3, currentSegmentDigits)) {
                            return emptyResult;
                        }
                        segments.set(3, currentSegmentValue);
                        currentSegmentValue = 0;
                        currentSegmentDigits = 0;
                        i = 4;
                    } else {
                        return emptyResult;
                    }
                } else if (i == 2) {
                    if (b == ' ' || b == 'T') {
                        if (!isValidDigits(i, currentSegmentDigits)) {
                            return emptyResult;
                        }
                        segments.set(i, currentSegmentValue);
                        currentSegmentValue = 0;
                        currentSegmentDigits = 0;
                        i += 1;
                    } else {
                        result[0] = new ArrayList<>();
                        result[1] = null;
                        result[2] = false;
                        return result;
                    }
                } else if (i == 3 || i == 4) {
                    if (b == ':') {
                        if (!isValidDigits(i, currentSegmentDigits)) {
                            return emptyResult;
                        }
                        segments.set(i, currentSegmentValue);
                        currentSegmentValue = 0;
                        currentSegmentDigits = 0;
                        i += 1;
                    } else {
                        return emptyResult;
                    }
                } else if (i == 5 || i == 6) {
                    if (b == '.' && i == 5) {
                        if (!isValidDigits(i, currentSegmentDigits)) {
                            return emptyResult;
                        }
                        segments.set(i, currentSegmentValue);
                        currentSegmentValue = 0;
                        currentSegmentDigits = 0;
                        i += 1;
                    } else {
                        if (!isValidDigits(i, currentSegmentDigits)) {
                            return emptyResult;
                        }
                        segments.set(i, currentSegmentValue);
                        currentSegmentValue = 0;
                        currentSegmentDigits = 0;
                        i += 1;
                        tz = new String(bytes, j, bytes.length - j);
                        j = bytes.length - 1;
                    }
                    if (i == 6 && b != '.') {
                        i += 1;
                    }
                } else {
                    if (i < segments.size() && (b == ':' || b == ' ')) {
                        if (!isValidDigits(i, currentSegmentDigits)) {
                            return emptyResult;
                        }
                        segments.set(i, currentSegmentValue);
                        currentSegmentValue = 0;
                        currentSegmentDigits = 0;
                        i += 1;
                    } else {
                        return emptyResult;
                    }
                }
            } else {
                if (i == 6) {
                    digitsMilli += 1;
                }
                // We will truncate the nanosecond part if there are more than 6 digits, which results
                // in loss of precision
                if (i != 6 || currentSegmentDigits < 6) {
                    currentSegmentValue = currentSegmentValue * 10 + parsedValue;
                }
                currentSegmentDigits += 1;
            }
            j += 1;
        }
        if (!isValidDigits(i, currentSegmentDigits)) {
            return emptyResult;
        }
        segments.set(i, currentSegmentValue);

        while (digitsMilli < 6) {
            segments.set(6, segments.get(6) * 10);
            digitsMilli += 1;
        }

        // This step also validates time zone part
        ZoneId zoneId = null;
        if (tz != null) {
            zoneId = getZoneId(tz.trim());
        }
        if (yearSign == null) {
            yearSign = 1;
        }
        segments.set(0, segments.get(0) * yearSign);
        result[0] = segments;
        result[1] = zoneId;
        result[2] = justTime;
        return result;
    }

    private static boolean isValidDigits(int segment, int digits) {
        // A Long is able to represent a timestamp within [+-]200 thousand years
        int maxDigitsYear = 6;
        // For the nanosecond part, more than 6 digits is allowed, but will be truncated.
        return segment == 6 || (segment == 0 && digits >= 4 && digits <= maxDigitsYear) ||
                // For the zoneId segment(7), it's could be zero digits when it's a region-based zone ID
                (segment == 7 && digits <= 2) ||
                (segment != 0 && segment != 6 && segment != 7 && digits > 0 && digits <= 2);
    }

    private static ZoneId getZoneId(String timeZoneId) {
        String formattedZoneId = timeZoneId
                // To support the (+|-)h:mm format because it was supported before Spark 3.0.
                .replaceFirst("(\\+|\\-)(\\d):", "$10$2:")
                // To support the (+|-)hh:m format because it was supported before Spark 3.0.
                .replaceFirst("(\\+|\\-)(\\d\\d):(\\d)$", "$1$2:0$3");

        return ZoneId.of(formattedZoneId, ZoneId.SHORT_IDS);
    }

    public static Long localDateTimeToMicros(LocalDateTime localDateTime) {
        return instantToMicros(localDateTime.toInstant(ZoneId.systemDefault().getRules().getOffset(Instant.now())));
    }

    public static Long instantToMicros(Instant instant) {
        long MIN_SECONDS = Math.floorDiv(Long.MIN_VALUE, 1000000L);
        long secs = instant.getEpochSecond();
        if (secs == MIN_SECONDS) {
            long us = Math.multiplyExact(secs + 1, 1000000L);
            return Math.addExact(us, NANOSECONDS.toMicros(instant.getNano()) - 1000000L);
        } else {
            long us = Math.multiplyExact(secs, 1000000L);
            return Math.addExact(us, NANOSECONDS.toMicros(instant.getNano()));
        }
    }


    public static LocalDateTime microsToLocalDateTime(Long micros) {
        return getLocalDateTime(micros, ZoneId.systemDefault().getRules().getOffset(Instant.now()));
    }

    public static LocalDateTime getLocalDateTime(Long micros, ZoneId zoneId) {
        return microsToInstant(micros).atZone(zoneId).toLocalDateTime();
    }

    public static Instant microsToInstant(Long micros) {
        long secs = Math.floorDiv(micros, 1000000L);
        // Unfolded Math.floorMod(us, MICROS_PER_SECOND) to reuse the result of
        // the above calculation of `secs` via `floorDiv`.
        long mos = micros - secs * 1000000L;
        return Instant.ofEpochSecond(secs, mos * 1000L);
    }




    public static void main(String[] args) {
        LocalDateTime localDateTime = stringToLocalDateTime("2021-01-01T12:34:56");
        localDateTime = localDateTime;
        System.out.println(localDateTimeToString(localDateTime));
    }

    /**
     * gave a datetime string and return the {@link DateTimeFormatter} which can be used to parse
     * it.
     *
     * @param dateTime eg: 2020-02-03 12:12:10.101
     * @return the DateTimeFormatter matched, will return null when not matched any pattern
     */
    public static DateTimeFormatter matchDateTimeFormatter(String dateTime) {
        if (dateTime.length() == 19) {
            for (Map.Entry<Pattern, DateTimeFormatter> entry :
                    YYYY_MM_DD_HH_MM_SS_19_FORMATTER_MAP_ENTRY_SET) {
                if (entry.getKey().matcher(dateTime).matches()) {
                    return entry.getValue();
                }
            }
        } else if (dateTime.length() > 19) {
            for (Map.Entry<Pattern, DateTimeFormatter> entry :
                    YYYY_MM_DD_HH_MM_SS_M19_FORMATTER_MAP_ENTRY_SET) {
                if (entry.getKey().matcher(dateTime).matches()) {
                    return entry.getValue();
                }
            }
        } else if (dateTime.length() == 14) {
            return YYYY_MM_DD_HH_MM_SS_14_FORMATTER;
        }
        return null;
    }

    public static LocalDateTime parse(String dateTime, DateTimeFormatter dateTimeFormatter) {
        TemporalAccessor parsedTimestamp = dateTimeFormatter.parse(dateTime);
        LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
        LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
        return LocalDateTime.of(localDate, localTime);
    }

    /**
     * gave a datetime string and return {@link LocalDateTime}
     *
     * <p>Due to the need to determine the rules of the formatter through regular expressions, there
     * will be a certain performance loss. When tested on 8c16g macos, the most significant
     * performance decrease compared to directly passing the formatter is
     * 'Pattern.compile("\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}.*")' has increased from 4.5
     * seconds to 10 seconds in a scenario where 1000w calculations are performed.
     *
     * <p>Analysis shows that there are two main reasons: one is that the regular expression
     * position in the map is 4, before this, three regular expression matches are required.
     *
     * <p>Another reason is to support the length of non fixed millisecond bits (minimum 0, maximum
     * 9), we used {@link DateTimeFormatter#ISO_LOCAL_TIME}, which also increases the time for time
     * conversion.
     *
     * @param dateTime eg: 2020-02-03 12:12:10.101
     * @return {@link LocalDateTime}
     */
    public static LocalDateTime parse(String dateTime) {
        DateTimeFormatter dateTimeFormatter = matchDateTimeFormatter(dateTime);
        return LocalDateTime.parse(dateTime, dateTimeFormatter);
    }

    public static LocalDateTime parse(String dateTime, Formatter formatter) {
        return LocalDateTime.parse(dateTime, FORMATTER_MAP.get(formatter));
    }

    public static LocalDateTime parse(long timestamp) {
        return parse(timestamp, ZoneId.systemDefault());
    }

    public static LocalDateTime parse(long timestamp, ZoneId zoneId) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return LocalDateTime.ofInstant(instant, zoneId);
    }

    public static String toString(LocalDateTime dateTime, Formatter formatter) {
        return dateTime.format(FORMATTER_MAP.get(formatter));
    }

    public static String toString(long timestamp, Formatter formatter) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return toString(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()), formatter);
    }

    public enum Formatter {
        YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_SSSSSS("yyyy-MM-dd HH:mm:ss.SSSSSS"),
        YYYY_MM_DD_HH_MM_SS_SPOT("yyyy.MM.dd HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_SLASH("yyyy/MM/dd HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_NO_SPLIT("yyyyMMddHHmmss"),
        YYYY_MM_DD_HH_MM_SS_ISO8601("yyyy-MM-dd'T'HH:mm:ss"),
        YYYY_MM_DD_HH_MM_SS_SSS_ISO8601("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        YYYY_MM_DD_HH_MM_SS_SSSSSS_ISO8601("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        YYYY_MM_DD_HH_MM_SS_SSSSSSSSS_ISO8601("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");

        private final String value;

        Formatter(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Formatter parse(String format) {
            Formatter[] formatters = Formatter.values();
            for (Formatter formatter : formatters) {
                if (formatter.getValue().equals(format)) {
                    return formatter;
                }
            }
            String errorMsg = String.format("Illegal format [%s]", format);
            throw new IllegalArgumentException(errorMsg);
        }
    }
}
