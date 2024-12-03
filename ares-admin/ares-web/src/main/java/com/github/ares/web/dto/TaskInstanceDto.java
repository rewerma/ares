package com.github.ares.web.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.github.ares.web.entity.TaskInstance;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class TaskInstanceDto {
    private Long id;

    private String batchCode;

    private String taskCode;

    private String taskName;

    @JsonFormat( pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime startTime;

    @JsonFormat( pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime endTime;

    private Integer status;

    private String executorHost;

    private String logPath;

    private String inParams;

    private String outParams;

    private String exeResult;

    @JsonFormat( pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createTime;

    public static TaskInstanceDto of(TaskInstance taskInstance) {
        TaskInstanceDto dto = new TaskInstanceDto();
        dto.setId(taskInstance.getId());
        dto.setBatchCode(taskInstance.getBatchCode());
        dto.setTaskCode(taskInstance.getTaskCode());
        dto.setTaskName(taskInstance.getTaskName());
        dto.setStartTime(taskInstance.getStartTime());
        dto.setEndTime(taskInstance.getEndTime());
        dto.setStatus(taskInstance.getStatus());
        dto.setExecutorHost(taskInstance.getExecutorHost());
        dto.setLogPath(taskInstance.getLogPath());
        dto.setInParams(taskInstance.getInParams());
        dto.setOutParams(taskInstance.getOutParams());
        dto.setExeResult(taskInstance.getExeResult());
        dto.setCreateTime(taskInstance.getCreateTime());
        return dto;
    }
}
