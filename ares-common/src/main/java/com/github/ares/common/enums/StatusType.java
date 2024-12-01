package com.github.ares.common.enums;

public enum StatusType {
    READY(1),
    SUBMIT(2),
    RUNNING(3),
    SUCCESS(4),
    FAILED(5),
    STOPPED(6);

    final int value;

    StatusType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
