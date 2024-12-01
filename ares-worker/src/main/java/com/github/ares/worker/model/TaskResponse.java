package com.github.ares.worker.model;

import com.github.ares.common.enums.StatusType;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class TaskResponse {
    private Integer processId;

    private Integer exitStatusCode;

    private Map<String, Object> outputParams;

    private List<Map<String, Object>> lastResult;

    private String logPath;

    private String errorMessage;

    private StatusType status;
}
