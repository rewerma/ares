package com.github.ares.connector.druid;

import java.util.ArrayList;
import java.util.List;

/**
 * Minimal JSON helpers for Druid SQL HTTP. Avoids a compile-time dependency on relocated Jackson
 * types ({@code com.github.ares.com.fasterxml.jackson}), which are only present after ares-jackson
 * is packaged.
 */
final class DruidJson {

    private DruidJson() {}

    static String sqlRequestBody(String sql) {
        return "{\"query\":"
                + quote(sql)
                + ",\"resultFormat\":\"array\",\"header\":true,\"typesHeader\":true}";
    }

    static String quote(String raw) {
        String value = raw == null ? "" : raw;
        StringBuilder out = new StringBuilder(value.length() + 8);
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    out.append("\\\"");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
            }
        }
        out.append('"');
        return out.toString();
    }

    static List<List<String>> parseArrayOfArrays(String json) {
        String text = json == null ? "[]" : json.trim();
        if (text.isEmpty()) {
            text = "[]";
        }
        Parser parser = new Parser(text);
        return parser.parseTable();
    }

    private static final class Parser {
        private final String text;
        private int index;

        private Parser(String text) {
            this.text = text;
        }

        private List<List<String>> parseTable() {
            skipWs();
            expect('[');
            List<List<String>> rows = new ArrayList<List<String>>();
            skipWs();
            if (peek() == ']') {
                index++;
                return rows;
            }
            while (true) {
                rows.add(parseRow());
                skipWs();
                char c = peek();
                if (c == ',') {
                    index++;
                    skipWs();
                    continue;
                }
                if (c == ']') {
                    index++;
                    return rows;
                }
                throw new IllegalStateException("Apache Druid SQL response is not a JSON array");
            }
        }

        private List<String> parseRow() {
            skipWs();
            expect('[');
            List<String> row = new ArrayList<String>();
            skipWs();
            if (peek() == ']') {
                index++;
                return row;
            }
            while (true) {
                row.add(parseValue());
                skipWs();
                char c = peek();
                if (c == ',') {
                    index++;
                    skipWs();
                    continue;
                }
                if (c == ']') {
                    index++;
                    return row;
                }
                throw new IllegalStateException("Apache Druid SQL response row is not a JSON array");
            }
        }

        private String parseValue() {
            skipWs();
            char c = peek();
            if (c == 'n' && text.startsWith("null", index)) {
                index += 4;
                return null;
            }
            if (c == '"') {
                return parseString();
            }
            if (c == 't' && text.startsWith("true", index)) {
                index += 4;
                return "true";
            }
            if (c == 'f' && text.startsWith("false", index)) {
                index += 5;
                return "false";
            }
            int start = index;
            if (c == '-') {
                index++;
            }
            while (index < text.length()) {
                char n = text.charAt(index);
                if ((n >= '0' && n <= '9') || n == '.' || n == 'e' || n == 'E' || n == '+') {
                    index++;
                    continue;
                }
                break;
            }
            if (index == start) {
                throw new IllegalStateException("Apache Druid SQL response has an unexpected token");
            }
            return text.substring(start, index);
        }

        private String parseString() {
            expect('"');
            StringBuilder out = new StringBuilder();
            while (index < text.length()) {
                char c = text.charAt(index++);
                if (c == '"') {
                    return out.toString();
                }
                if (c != '\\') {
                    out.append(c);
                    continue;
                }
                if (index >= text.length()) {
                    break;
                }
                char e = text.charAt(index++);
                switch (e) {
                    case '"':
                    case '\\':
                    case '/':
                        out.append(e);
                        break;
                    case 'b':
                        out.append('\b');
                        break;
                    case 'f':
                        out.append('\f');
                        break;
                    case 'n':
                        out.append('\n');
                        break;
                    case 'r':
                        out.append('\r');
                        break;
                    case 't':
                        out.append('\t');
                        break;
                    case 'u':
                        if (index + 4 <= text.length()) {
                            out.append((char) Integer.parseInt(text.substring(index, index + 4), 16));
                            index += 4;
                        }
                        break;
                    default:
                        out.append(e);
                }
            }
            throw new IllegalStateException("Apache Druid SQL response has an unterminated string");
        }

        private void skipWs() {
            while (index < text.length()) {
                char c = text.charAt(index);
                if (c == ' ' || c == '\n' || c == '\r' || c == '\t') {
                    index++;
                } else {
                    return;
                }
            }
        }

        private char peek() {
            if (index >= text.length()) {
                throw new IllegalStateException("Apache Druid SQL response ended early");
            }
            return text.charAt(index);
        }

        private void expect(char expected) {
            char c = peek();
            if (c != expected) {
                throw new IllegalStateException("Apache Druid SQL response is not valid JSON");
            }
            index++;
        }
    }
}
