package com.github.ares.web.enums;

import lombok.Getter;

public enum EngineType {
    LOCAL("local", "ares-local-starter.sh"),
    SPARK2("spark2", "start-ares-spark2-connector.sh"),
    SPARK3("spark3", "start-ares-spark3-connector.sh");

    @Getter
    final String value;
    @Getter
    final String scriptFile;

    EngineType(String value, String scriptFile) {
        this.value = value;
        this.scriptFile = scriptFile;
    }

    public static EngineType fromValue(String value) {
        for (EngineType engineType : EngineType.values()) {
            if (engineType.getValue().equalsIgnoreCase(value)) {
                return engineType;
            }
        }
        return LOCAL;
    }
}
