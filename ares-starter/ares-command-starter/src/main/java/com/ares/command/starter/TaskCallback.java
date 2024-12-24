package com.ares.command.starter;


import com.ares.command.starter.model.TaskResponse;

public interface TaskCallback {
    void running(String logPath);

    void completed(TaskResponse response);
}
