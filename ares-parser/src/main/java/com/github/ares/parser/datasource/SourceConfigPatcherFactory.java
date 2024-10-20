package com.github.ares.parser.datasource;

import java.util.HashMap;
import java.util.Map;

public class SourceConfigPatcherFactory {
    private SourceConfigPatcherFactory() {
    }

    private static final Map<String, SourceConfigPatcher> sourceConfigPatchers = new HashMap<>();

    public static synchronized void register(String name, SourceConfigPatcher sourceConfigPatcher) {
        sourceConfigPatchers.put(name, sourceConfigPatcher);
    }

    public static SourceConfigPatcher getSourceConfigPatcher(String name) {
        SourceConfigPatcher sourceConfigPatcher = sourceConfigPatchers.get(name);
        if (sourceConfigPatcher == null) {
            throw new IllegalArgumentException("No SourceConfigPatcher found for name: " + name);
        }
        return sourceConfigPatcher;
    }
}
