package com.github.ares.api.common;

public class ExecutionEngineType {
    public static EngineType engineType;

    public static EngineTypeVersion engineTypeVersion;

    public static void init(EngineType et, EngineTypeVersion etv) {
        engineType = et;
        engineTypeVersion = etv;
    }
}
