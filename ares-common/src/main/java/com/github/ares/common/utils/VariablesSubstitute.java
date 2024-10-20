package com.github.ares.common.utils;

import org.apache.commons.lang3.text.StrSubstitutor;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class VariablesSubstitute {

    private VariablesSubstitute() {}

    /**
     * @param text raw string
     * @param timeFormat example : "yyyy-MM-dd HH:mm:ss"
     * @return replaced text
     */
    public static String substitute(String text, String timeFormat) {
        DateTimeFormatter df = DateTimeFormatter.ofPattern(timeFormat);
        final String formattedDate = df.format(ZonedDateTime.now());

        final Map<String, String> valuesMap = new HashMap<>(3);
        valuesMap.put(Constants.UUID, UUID.randomUUID().toString());
        valuesMap.put(Constants.NOW, formattedDate);
        valuesMap.put(timeFormat, formattedDate);
        return substitute(text, valuesMap);
    }

    /**
     * @param text raw string
     * @param valuesMap key is variable name, value is substituted string.
     * @return replaced text
     */
    public static String substitute(String text, Map<String, String> valuesMap) {
        final StrSubstitutor sub = new StrSubstitutor(valuesMap);
        return sub.replace(text);
    }
}
