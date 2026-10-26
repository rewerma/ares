package com.github.ares.connector.jdbc.utils;

import java.util.regex.Pattern;

public final class WhereConditionValidator {

    private static final Pattern STARTS_WITH_WHERE =
            Pattern.compile("^where\\s+.+", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern FORBIDDEN_KEYWORDS =
            Pattern.compile(
                    "\\b(select|insert|update|delete|drop|truncate|alter|create|union|exec|execute|"
                            + "grant|revoke|merge|call|into|from|load|replace|show|describe|explain|"
                            + "set|use|sleep|benchmark|information_schema|pg_sleep|xp_cmdshell)\\b",
                    Pattern.CASE_INSENSITIVE);

    private static final Pattern ALLOWED_CHARACTERS =
            Pattern.compile("^[\\w\\s.,'\"=<>!()+\\-%*/|&\\[\\]:@#`]+$");

    private WhereConditionValidator() {}

    public static String validateAndNormalize(String whereConditionClause) {
        if (whereConditionClause == null || whereConditionClause.trim().isEmpty()) {
            throw new IllegalArgumentException("where_condition must not be blank");
        }
        String trimmed = whereConditionClause.trim();
        if (!STARTS_WITH_WHERE.matcher(trimmed).matches()) {
            throw new IllegalArgumentException(
                    "The where condition clause must start with 'where'. value: "
                            + whereConditionClause);
        }
        if (trimmed.indexOf(';') >= 0) {
            throw new IllegalArgumentException("where_condition must not contain ';'");
        }
        if (trimmed.contains("--") || trimmed.contains("/*") || trimmed.contains("*/")) {
            throw new IllegalArgumentException("where_condition must not contain SQL comments");
        }
        if (FORBIDDEN_KEYWORDS.matcher(trimmed).find()) {
            throw new IllegalArgumentException(
                    "where_condition contains forbidden SQL keyword: " + whereConditionClause);
        }
        if (!ALLOWED_CHARACTERS.matcher(trimmed).matches()) {
            throw new IllegalArgumentException(
                    "where_condition contains disallowed characters: " + whereConditionClause);
        }
        return trimmed;
    }
}
