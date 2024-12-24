package com.ares.command.starter;

import com.ares.command.starter.model.TaskContext;

public interface TaskExecutor {
    void start(TaskContext taskContext);

    void executeTask(TaskContext taskContext);

    void stop(TaskContext taskContext);

    String getFullLog(TaskContext taskContext);

    void registerCallback(TaskCallback callback);
}
