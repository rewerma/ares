package com.github.ares.sql.function.utils;

import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.utils.DateTimeUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Utils {
    public static LocalDate toLocalDate(Object value) {
        if (value instanceof String) {
            LocalDateTime localDateTime = toLocalDateTime(value);
            if (localDateTime == null) {
                return null;
            }
            return localDateTime.toLocalDate();
        } else if (value instanceof LocalDate) {
            return ((LocalDate) value);
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalDate();
        }
        throw new AresException("cannot resolve to data type mismatch: " +
                "argument requires Date type, however, " + value + " is of " + value.getClass().getSimpleName() + " type");
    }

    public static LocalDateTime toLocalDateTime(Object value) {
        if (value instanceof String) {
            try {
                return DateTimeUtils.stringToLocalDateTime((String) value);
            } catch (AresException e) {
                return null;
            }
        } else if (value instanceof LocalDate) {
            return ((LocalDate) value).atTime(0, 0);
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value);
        }
        throw new AresException("cannot resolve to data type mismatch: " +
                "argument requires Timestamp type, however, " + value + " is of " + value.getClass().getSimpleName() + " type");
    }

    public static String toStr(Object value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    public static Number toNumber(Object value) {
        if (value == null) {
            return null;
        }
        Number val = null;
        if (value instanceof String) {
            try {
                val = Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                // ignore
            }
        } else if (value instanceof Number) {
            val = (Number) value;
        } else {
            throw new AresException("cannot resolve " + value + " due to number type");
        }
        return val;
    }
}
