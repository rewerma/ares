package com.github.ares.connector.druid;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

/**
 * Parses Apache Druid SQL HTTP temporal cells. {@code typesHeader} may say {@code LONG} for
 * {@code __time}, while the array payload uses ISO-8601 UTC instants such as
 * {@code 2023-03-09T12:23:00.000Z}. Ares {@code LocalDateTime} values use the UTC wall clock of
 * that instant (not the JVM default zone).
 */
final class DruidTimeValues {

    private static final Pattern EPOCH = Pattern.compile("-?\\d+");
    private static final Pattern ISO_OFFSET = Pattern.compile(".*[+-]\\d{2}:?\\d{2}$");
    private static final Pattern ISO_DATE_TIME =
            Pattern.compile("\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}.*");

    private DruidTimeValues() {}

    static boolean isEpochNumber(String raw) {
        return raw != null && EPOCH.matcher(raw.trim()).matches();
    }

    static boolean isIsoDateTime(String raw) {
        return raw != null && ISO_DATE_TIME.matcher(raw.trim()).matches();
    }

    static Instant parseInstant(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Druid timestamp value is blank");
        }
        if (isEpochNumber(trimmed)) {
            return instantFromEpoch(Long.parseLong(trimmed));
        }
        String text = trimmed.indexOf(' ') > 0 && trimmed.indexOf('T') < 0
                ? trimmed.replace(' ', 'T')
                : trimmed;
        try {
            if (text.endsWith("Z") || text.endsWith("z")) {
                return Instant.parse(text);
            }
            if (ISO_OFFSET.matcher(text).matches()) {
                return OffsetDateTime.parse(text).toInstant();
            }
            return LocalDateTime.parse(text).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException ex) {
            throw new IllegalArgumentException("Cannot parse Druid timestamp value: " + raw, ex);
        }
    }

    static LocalDateTime toLocalDateTime(String raw) {
        Instant instant = parseInstant(raw);
        return instant == null ? null : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    static LocalDate toLocalDate(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Druid date value is blank");
        }
        if (isEpochNumber(trimmed)) {
            return LocalDateTime.ofInstant(instantFromEpoch(Long.parseLong(trimmed)), ZoneOffset.UTC)
                    .toLocalDate();
        }
        if (!isIsoDateTime(trimmed) && trimmed.length() >= 10 && trimmed.charAt(4) == '-') {
            try {
                return LocalDate.parse(trimmed.substring(0, 10));
            } catch (DateTimeException ex) {
                throw new IllegalArgumentException("Cannot parse Druid date value: " + raw, ex);
            }
        }
        return LocalDateTime.ofInstant(parseInstant(trimmed), ZoneOffset.UTC).toLocalDate();
    }

    static Instant instantFromEpoch(long epoch) {
        if (Math.abs(epoch) < 100_000_000_000L) {
            return Instant.ofEpochSecond(epoch);
        }
        return Instant.ofEpochMilli(epoch);
    }
}
