package com.github.ares.engine.utils;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.com.fasterxml.jackson.core.JsonProcessingException;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.core.PlParams;
import com.github.ares.parser.utils.PLParserUtil;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class EngineUtil {
    public static String replaceParams(String expr, PlParams plParams) {
        for (Map.Entry<String, Serializable> entry : plParams.entrySet()) {
            String param = entry.getKey();
            Object value = entry.getValue();
            PlType type = plParams.getType(param);

            if (value == null) {
                expr = expr.replace("\"${" + param + "}\"", "null");
            } else if (InternalFieldType.DATE == type.getType()) {
                expr = expr.replace("\"${" + param + "}\"", "TO_DATE(" + value + ")");
            } else if (InternalFieldType.TIMESTAMP == type.getType()) {
                expr = expr.replace("\"${" + param + "}\"", "TO_TIMESTAMP(" + value + ")");
            } else if (InternalFieldType.BYTES == type.getType()) {
                expr = expr.replace("\"${" + param + "}\"", "UNHEX(" + value + ")");
            } else {
                expr = expr.replace("\"${" + param + "}\"", String.valueOf(value));
            }
        }
        return expr;
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
