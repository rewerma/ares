package com.github.ares.web.worker;

import com.github.ares.web.dto.TaskContext;
import com.github.ares.web.dto.TaskResponse;

import java.util.function.Consumer;

public interface TaskWorker {
    void start(TaskContext taskContext);

    void stop(TaskContext taskContext);

    String getFullLog(TaskContext taskContext);

    void registerCallback(Callback callback);
}
