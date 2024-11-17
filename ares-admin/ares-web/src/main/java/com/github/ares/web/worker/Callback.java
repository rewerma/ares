package com.github.ares.web.worker;

import com.github.ares.web.dto.TaskResponse;

public interface Callback {
    void running(String logPath);

    void completed(TaskResponse response);
}
