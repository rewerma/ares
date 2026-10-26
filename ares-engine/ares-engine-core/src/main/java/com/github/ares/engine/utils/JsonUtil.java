package com.github.ares.engine.utils;

import static com.github.ares.com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT;
import static com.github.ares.com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.github.ares.com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL;

import com.github.ares.com.fasterxml.jackson.annotation.JsonInclude;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {
    public static ObjectMapper getJsonMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
                .configure(READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
                .setSerializationInclusion(JsonInclude.Include.ALWAYS)
                .setDateFormat(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
                .setTimeZone(java.util.TimeZone.getTimeZone("GMT+8"));
        return mapper;
    }
}
