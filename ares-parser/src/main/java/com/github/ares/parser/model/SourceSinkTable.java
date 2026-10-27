package com.github.ares.parser.model;

import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import lombok.Getter;

@Getter
public class SourceSinkTable implements Serializable {
    private static final long serialVersionUID = -1L;

    private final Map<String, LogicalCreateSourceTable> sourceTables = new LinkedHashMap<>();
    private final Map<String, LogicalCreateSinkTable> sinkTables = new LinkedHashMap<>();

    /** Script alias to the quoted Spark catalog table for Hive and Paimon. */
    private final Map<String, String> catalogAliases = new LinkedHashMap<>();

    public void cacheCatalogAlias(String alias, String quotedTable) {
        if (alias == null || alias.isEmpty() || quotedTable == null || quotedTable.isEmpty()) {
            return;
        }
        catalogAliases.put(normalizeAlias(alias), quotedTable);
    }

    public String cachedCatalogTable(String alias) {
        return alias == null ? null : catalogAliases.get(normalizeAlias(alias));
    }

    /** Identifier text with quotes removed, case preserved. */
    public static String plainName(String name) {
        String[] parts = name.split("\\.");
        StringBuilder key = new StringBuilder();
        for (String part : parts) {
            if (key.length() > 0) {
                key.append('.');
            }
            key.append(unquote(part));
        }
        return key.toString();
    }

    public static String normalizeAlias(String alias) {
        return plainName(alias).toLowerCase(Locale.ROOT);
    }

    private static String unquote(String text) {
        String trimmed = text == null ? "" : text.trim();
        if (trimmed.length() >= 2) {
            char open = trimmed.charAt(0);
            char close = trimmed.charAt(trimmed.length() - 1);
            if ((open == '`' && close == '`') || (open == '"' && close == '"')) {
                return trimmed.substring(1, trimmed.length() - 1);
            }
        }
        return trimmed;
    }
}
