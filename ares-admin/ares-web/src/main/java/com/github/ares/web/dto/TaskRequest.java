package com.github.ares.web.dto;

import lombok.Data;

import java.util.Map;

@Data
public class TaskRequest {
    private Long taskInstanceId;

    private String executePath;

    private String logPath;

    private Integer processId;

    private Map<String, String> environments;
}
