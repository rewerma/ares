package com.github.ares.web.dto;

import lombok.Data;

@Data
public class TaskRequest {
    private Long taskInstanceId;

    private String executePath;

    private String logPath;

    private Integer processId;
}
