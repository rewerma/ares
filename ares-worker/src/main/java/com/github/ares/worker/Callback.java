package com.github.ares.worker;

import com.github.ares.worker.model.TaskResponse;

public interface Callback {
    void running(String logPath);

    void completed(TaskResponse response);
}
