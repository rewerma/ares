package com.github.ares.parser.datasource;

import java.util.HashMap;
import java.util.Map;

public class SourceConfigComplementFactory {
    private SourceConfigComplementFactory() {
    }

    private static final Map<String, SourceConfigComplement> sourceConfigPatchers = new HashMap<>();

    public static synchronized void register(String name, SourceConfigComplement sourceConfigPatcher) {
        sourceConfigPatchers.put(name, sourceConfigPatcher);
    }

    public static SourceConfigComplement getSourceConfigComplement(String name) {
        SourceConfigComplement sourceConfigPatcher = sourceConfigPatchers.get(name);
        if (sourceConfigPatcher == null) {
            throw new IllegalArgumentException("No SourceConfigPatcher found for name: " + name);
        }
        return sourceConfigPatcher;
    }
}
