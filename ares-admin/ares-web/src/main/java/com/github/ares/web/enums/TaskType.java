package com.github.ares.web.enums;

public enum TaskType {
    ARES("ARES", "shellWorker"),
    SHELL("ARES", "shellWorker");

    String name;

    String beanName;

    TaskType(String name, String beanName) {
        this.name = name;
        this.beanName = beanName;
    }

    public String getName() {
        return name;
    }

    public String getBeanName() {
        return beanName;
    }
}
