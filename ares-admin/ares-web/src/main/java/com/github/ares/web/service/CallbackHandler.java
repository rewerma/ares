package com.github.ares.web.service;

import com.github.ares.common.enums.StatusType;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.web.entity.TaskDefinition;
import com.github.ares.web.entity.TaskInstance;
import com.github.ares.worker.Callback;
import com.github.ares.worker.model.TaskResponse;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
public class CallbackHandler implements Callback {
    private final TaskDefinition taskDefinition;
    private final TaskInstance taskInstance;

    public CallbackHandler(TaskDefinition taskDefinition, TaskInstance taskInstance) {
        this.taskDefinition = taskDefinition;
        this.taskInstance = taskInstance;
    }

    @Override
    public void running(String logPath) {
        taskInstance.setLogPath(logPath);
        taskInstance.setStatus(StatusType.RUNNING.getValue());
        taskInstance.update("logPath", "status");
    }

    @Override
    public void completed(TaskResponse taskResponse) {
        if (taskResponse.getStatus() == StatusType.SUCCESS) {
            Map<String, Object> outputParams = taskResponse.getOutputParams();
            if (taskDefinition.getOutParams() != null && outputParams != null) {
                try {
                    Map<String, Object> defOutParams = JsonUtils.toMap2(taskDefinition.getOutParams());
                    outputParams.forEach((k, v) -> {
                        if (defOutParams.containsKey(k)) {
                            defOutParams.put(k, v);
                        }
                    });
                    taskInstance.setOutParams(JsonUtils.toJsonString(defOutParams));
                } catch (Exception e) {
                    log.error("outParams format error: {}", e.getMessage());
                }
            }
            List<Map<String, Object>> resultJson = taskResponse.getLastResult();
            if (resultJson != null) {
                String result = JsonUtils.toJsonString(resultJson);
                taskInstance.setExeResult(result);
            }
            taskInstance.setStatus(StatusType.SUCCESS.getValue());
            taskInstance.setEndTime(LocalDateTime.now());
            taskInstance.setLogPath(taskResponse.getLogPath());
            taskInstance.update("status", "outParams", "endTime", "exeResult", "logPath");
        } else if (taskResponse.getStatus() == StatusType.FAILED) {
            taskInstance.setStatus(StatusType.FAILED.getValue());
            taskInstance.setEndTime(LocalDateTime.now());
            taskInstance.setExeResult(taskResponse.getErrorMessage());
            taskInstance.update("status", "endTime", "exeResult");
        } else if (taskResponse.getStatus() == StatusType.STOPPED) {
            taskInstance.setStatus(StatusType.STOPPED.getValue());
            taskInstance.setEndTime(LocalDateTime.now());
            taskInstance.update("status", "endTime");
        } else {
            log.error("task execution type error: {}", taskResponse.getStatus());
            taskInstance.setStatus(StatusType.FAILED.getValue());
            taskInstance.setEndTime(LocalDateTime.now());
            taskInstance.setExeResult("task execution callback type: " + taskResponse.getStatus().name());
            taskInstance.update("status", "endTime", "exeResult");
        }
    }
}