package com.github.ares.web.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class TaskContext {
    private String taskCode;

    private String taskName;

    private Long taskInstanceId;

    private String taskContent;

    private String systemEnvParam;

    private String envParams;

    private String inParams;

    private String outParams;

    private String logPath;

    private Integer status;

    private Map<String, Object> outputValues;

    private List<Map<String, Object>> lastResult;

    private String errorMessage;

}
