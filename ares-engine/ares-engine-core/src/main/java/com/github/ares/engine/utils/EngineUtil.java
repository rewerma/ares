package com.github.ares.engine.utils;

import static com.github.ares.common.utils.DateTimeUtils.DATE_FORMATTER;

import com.github.ares.com.fasterxml.jackson.core.JsonProcessingException;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.utils.DateTimeUtils;
import com.github.ares.engine.core.PlParams;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;

public class EngineUtil {
    public static String replaceParams(String expr, PlParams plParams) {
        List<Map.Entry<String, Serializable>> entries = new ArrayList<>(plParams.entrySet());
        entries.sort(
                (left, right) -> Integer.compare(right.getKey().length(), left.getKey().length()));
        for (Map.Entry<String, Serializable> entry : entries) {
            String param = entry.getKey();
            Object value = entry.getValue();
            PlType type = plParams.getType(param);

            if (value == null) {
                expr = expr.replace("\"${" + param + "}\"", "null");
            } else if (type != null && InternalFieldType.DATE == type.getType()) {
                expr = expr.replace("\"${" + param + "}\"", "TO_DATE(" + value + ")");
            } else if (type != null && InternalFieldType.TIMESTAMP == type.getType()) {
                expr = expr.replace("\"${" + param + "}\"", "TO_TIMESTAMP(" + value + ")");
            } else if (type != null && InternalFieldType.BYTES == type.getType()) {
                expr = expr.replace("\"${" + param + "}\"", "UNHEX(" + value + ")");
            } else {
                expr = expr.replace("\"${" + param + "}\"", String.valueOf(value));
            }
        }
        return expr;
    }

    public static Map<String, Object> toEvalParams(PlParams plParams) {
        if (plParams == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> params = new LinkedHashMap<>();
        for (Map.Entry<String, Serializable> entry : plParams.entrySet()) {
            params.put(
                    entry.getKey(),
                    unwrapForExpression(entry.getValue(), plParams.getType(entry.getKey())));
        }
        return params;
    }

    private static Object unwrapForExpression(Serializable value, PlType type) {
        if (value == null) {
            return null;
        }
        if (type == null) {
            return unwrapQuoted(value);
        }
        switch (type.getType()) {
            case VARCHAR:
                return unwrapQuoted(value);
            case DATE:
                if (value instanceof LocalDate) {
                    return value;
                }
                if (value instanceof LocalDateTime) {
                    return ((LocalDateTime) value).toLocalDate();
                }
                return parseDate(unwrapQuoted(value));
            case TIMESTAMP:
                if (value instanceof LocalDateTime) {
                    return value;
                }
                if (value instanceof LocalDate) {
                    return ((LocalDate) value).atStartOfDay();
                }
                return DateTimeUtils.stringToLocalDateTime(unwrapQuoted(value));
            case BYTES:
                if (value instanceof byte[]) {
                    return value;
                }
                return hexToBytes(unwrapQuoted(value));
            default:
                return value;
        }
    }

    private static LocalDate parseDate(String text) {
        try {
            return LocalDate.parse(text, DATE_FORMATTER);
        } catch (Exception e) {
            return DateTimeUtils.stringToLocalDateTime(text).toLocalDate();
        }
    }

    private static String unwrapQuoted(Object value) {
        String text = value.toString();
        if (text.length() >= 2
                && text.charAt(0) == '\''
                && text.charAt(text.length() - 1) == '\'') {
            return text.substring(1, text.length() - 1).replace("''", "'");
        }
        return text;
    }

    private static byte[] hexToBytes(String hex) {
        if (hex == null) {
            return null;
        }
        int len = hex.length();
        if ((len & 1) == 1) {
            hex = "0" + hex;
            len++;
        }
        byte[] bytes = new byte[len / 2];
        for (int i = 0; i < bytes.length; i++) {
            int hi = Character.digit(hex.charAt(i * 2), 16);
            int lo = Character.digit(hex.charAt(i * 2 + 1), 16);
            if (hi < 0 || lo < 0) {
                return null;
            }
            bytes[i] = (byte) ((hi << 4) + lo);
        }
        return bytes;
    }

    public static String appendQuoteIdentifier(Object value) {
        if (value == null) {
            return null;
        }
        return "'" + value + "'";
    }

    public static String convertQuoteIdentifier(String value) {
        if (value == null) {
            return null;
        }
        return value.replace("'", "''");
    }

    public static String handleQuoteIdentifier(Object value) {
        String res = null;
        if (value instanceof String) {
            res = convertQuoteIdentifier((String) value);
        }
        if (res != null) {
            res = appendQuoteIdentifier(res);
        } else {
            res = appendQuoteIdentifier(value);
        }
        return res;
    }

    public static String exceptionHandler(Exception e, Logger logger, Map<String, Object> exData) {
        logger.error(e.getMessage(), e);
        Map<String, Object> errorInfo = new LinkedHashMap<>();
        errorInfo.put("code", 500);
        errorInfo.put("message", e.getMessage());
        ObjectMapper mapper = JsonUtil.getJsonMapper();
        if (exData != null) {
            errorInfo.put("data", exData);
        }
        try {
            return mapper.writeValueAsString(errorInfo);
        } catch (JsonProcessingException ex) {
            throw new AresException(ex);
        }
    }
}
