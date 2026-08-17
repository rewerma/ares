package com.github.ares.connector.hive.utils;

import com.github.ares.common.exceptions.CommonError;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@UtilityClass
public class HiveTypeConverter {

    public static String convert(String pluginName, String columnName, String hiveType) {
        try {
            return convertInternal(hiveType.trim());
        } catch (RuntimeException e) {
            throw CommonError.convertToAresTypeError(pluginName, hiveType, columnName);
        }
    }

    private static String convertInternal(String hiveType) {
        String normalized = hiveType.toLowerCase(Locale.ROOT);
        if (normalized.startsWith("struct<")) {
            return convertStruct(hiveType);
        }
        if (normalized.startsWith("array<")) {
            return "array<" + convertInternal(extractGeneric(hiveType)) + ">";
        }
        if (normalized.startsWith("map<")) {
            String generic = extractGeneric(hiveType);
            int commaIndex = indexOfTopLevelDelimiter(generic, ',');
            if (commaIndex < 0) {
                throw new IllegalArgumentException("Invalid map type: " + hiveType);
            }
            String keyType = generic.substring(0, commaIndex).trim();
            String valueType = generic.substring(commaIndex + 1).trim();
            return "map<" + convertInternal(keyType) + "," + convertInternal(valueType) + ">";
        }
        if (normalized.startsWith("decimal")) {
            return hiveType.toLowerCase(Locale.ROOT);
        }
        if (normalized.startsWith("varchar")
                || normalized.startsWith("char")
                || normalized.equals("string")) {
            return "string";
        }
        if (normalized.contains("binary")) {
            return "bytes";
        }
        if (normalized.equals("tinyint")) {
            return "tinyint";
        }
        if (normalized.equals("smallint")) {
            return "smallint";
        }
        if (normalized.equals("int") || normalized.equals("integer")) {
            return "int";
        }
        if (normalized.equals("bigint")) {
            return "bigint";
        }
        if (normalized.equals("float")) {
            return "float";
        }
        if (normalized.equals("double")) {
            return "double";
        }
        if (normalized.equals("boolean")) {
            return "boolean";
        }
        if (normalized.equals("date")) {
            return "date";
        }
        if (normalized.startsWith("timestamp")) {
            return "timestamp";
        }
        throw new IllegalArgumentException("Unsupported hive type: " + hiveType);
    }

    private static String convertStruct(String hiveType) {
        String body = extractGeneric(hiveType);
        List<String> fieldDecls = splitTopLevel(body, ',');
        StringBuilder structBuilder = new StringBuilder("{");
        for (int i = 0; i < fieldDecls.size(); i++) {
            String fieldDecl = fieldDecls.get(i);
            int colonIndex = indexOfTopLevelDelimiter(fieldDecl, ':');
            if (colonIndex < 0) {
                throw new IllegalArgumentException("Invalid struct field: " + fieldDecl);
            }
            String fieldName = fieldDecl.substring(0, colonIndex).trim();
            String fieldType = fieldDecl.substring(colonIndex + 1).trim();
            if (i > 0) {
                structBuilder.append(',');
            }
            structBuilder.append(fieldName).append(':').append(convertInternal(fieldType));
        }
        structBuilder.append('}');
        return structBuilder.toString();
    }

    private static String extractGeneric(String hiveType) {
        int start = hiveType.indexOf('<');
        int end = hiveType.lastIndexOf('>');
        if (start < 0 || end <= start) {
            throw new IllegalArgumentException("Invalid generic hive type: " + hiveType);
        }
        return hiveType.substring(start + 1, end);
    }

    private static int indexOfTopLevelDelimiter(String value, char delimiter) {
        int depth = 0;
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (current == '<') {
                depth++;
            } else if (current == '>') {
                depth--;
            } else if (current == delimiter && depth == 0) {
                return i;
            }
        }
        return -1;
    }

    private static List<String> splitTopLevel(String value, char delimiter) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (current == '<') {
                depth++;
            } else if (current == '>') {
                depth--;
            } else if (current == delimiter && depth == 0) {
                parts.add(value.substring(start, i).trim());
                start = i + 1;
            }
        }
        if (start <= value.length()) {
            parts.add(value.substring(start).trim());
        }
        return parts;
    }
}
