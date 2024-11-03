package com.github.ares.common.utils;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

public class TimeUtils {
    private static final Map<Formatter, DateTimeFormatter> FORMATTER_MAP = new EnumMap<>(Formatter.class);

    static {
        FORMATTER_MAP.put(
                Formatter.HH_MM_SS, DateTimeFormatter.ofPattern(Formatter.HH_MM_SS.value));
        FORMATTER_MAP.put(
                Formatter.HH_MM_SS_SSS, DateTimeFormatter.ofPattern(Formatter.HH_MM_SS_SSS.value));
    }

    public static LocalTime parse(String time, Formatter formatter) {
        return LocalTime.parse(time, FORMATTER_MAP.get(formatter));
    }

    public static String toString(LocalTime time, Formatter formatter) {
        return time.format(FORMATTER_MAP.get(formatter));
    }

    public enum Formatter {
        HH_MM_SS("HH:mm:ss"),
        HH_MM_SS_SSS("HH:mm:ss.SSS");
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
