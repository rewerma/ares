package com.github.ares.format.json;

import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.Options;

import java.util.Map;

public class JsonFormatOptions {
    public static final Option<Boolean> FAIL_ON_MISSING_FIELD =
            Options.key("fail-on-missing-field")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to specify whether to fail if a field is missing or not, false by default.");

    public static final Option<Boolean> IGNORE_PARSE_ERRORS =
            Options.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors, false by default.");

    public static boolean getFailOnMissingField(Map<String, String> options) {
        return Boolean.parseBoolean(
                options.getOrDefault(
                        FAIL_ON_MISSING_FIELD.key(), FAIL_ON_MISSING_FIELD.toString()));
    }

    public static boolean getIgnoreParseErrors(Map<String, String> options) {
        return Boolean.parseBoolean(
                options.getOrDefault(IGNORE_PARSE_ERRORS.key(), IGNORE_PARSE_ERRORS.toString()));
    }
}
