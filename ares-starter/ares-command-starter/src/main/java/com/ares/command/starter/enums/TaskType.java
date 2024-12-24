package com.ares.command.starter.enums;

public enum TaskType {
    ARES("ARES", "aresWorker"),
    SHELL("SHELL", "shellWorker");

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
