package com.github.ares.common.enums;

public enum TaskType {
    ARES("ARES"),
    SHELL("SHELL");

    String name;

    TaskType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
