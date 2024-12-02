package com.github.ares.web.dto;

import lombok.Data;

@Data
public class TaskExecutionDto {
    private String taskCode;

    private String batchCode;

    private String inParams;
}
