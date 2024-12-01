package com.github.ares.worker;

import com.github.ares.worker.model.TaskContext;

public interface TaskWorker {
    void start(TaskContext taskContext);

    void executeTask(TaskContext taskContext);

    void stop(TaskContext taskContext);

    String getFullLog(TaskContext taskContext);

    void registerCallback(Long key, Callback callback);
}
