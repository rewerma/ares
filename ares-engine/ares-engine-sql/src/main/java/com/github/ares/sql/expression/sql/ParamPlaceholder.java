package com.github.ares.sql.expression.sql;

/**
 * Parser emits PL parameters as quoted identifiers {@code "${name}"}. JSQLParser keeps the quotes
 * in {@code Column.getColumnName()}.
 */
final class ParamPlaceholder {
    private ParamPlaceholder() {}

    static String nameOf(String identifier) {
        if (identifier == null) {
            return null;
        }
        String token = identifier.trim();
        if (token.length() >= 5 && token.startsWith("\"${") && token.endsWith("}\"")) {
            return token.substring(3, token.length() - 2);
        }
        if (token.length() >= 4 && token.startsWith("${") && token.endsWith("}")) {
            return token.substring(2, token.length() - 1);
        }
        return null;
    }
}
