package com.github.ares.worker.model;

import lombok.Data;

@Data
public class TaskConfig {
    private String aresHome;

    private String engineType;

    private String sparkHome;

    private Integer threadPoolSize = 100;
}
