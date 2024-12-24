package com.ares.command.starter.worker;

import com.ares.command.starter.TaskExecutor;
import com.ares.command.starter.config.AresConfig;
import com.ares.command.starter.enums.TaskType;
import com.ares.command.starter.model.TaskContext;
import com.github.ares.com.google.common.collect.Maps;
import com.github.ares.common.exceptions.AresException;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class WorkerExecution {

    private final LinkedBlockingQueue<TaskContext> taskExecutionQueue = new LinkedBlockingQueue<>();

    private ExecutorService taskExecutorPool;

    private volatile boolean isRunning = false;

    private Map<String, TaskExecutor> taskExecutors;

    public void init() {
        AresConfig aresConfig = AresConfig.getConfig();
        int threadPoolSize = aresConfig.getThreadPoolSize() == null ? 50 : aresConfig.getThreadPoolSize();
        this.taskExecutors = Maps.newConcurrentMap();
        taskExecutorPool = Executors.newFixedThreadPool(threadPoolSize);
        isRunning = true;
        run();
    }

    private void run() {
        CompletableFuture.runAsync(() -> {
            while (isRunning) {
                try {
                    TaskContext taskContext = taskExecutionQueue.take();
                    // execute task
                    taskExecutorPool.submit(() -> {
                        TaskExecutor taskWorker = getTaskExecutor(taskContext.getTaskType());
                        taskWorker.executeTask(taskContext);
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, Executors.newSingleThreadExecutor());
    }

    public void start(TaskContext taskContext) {
        taskExecutionQueue.add(taskContext);
    }

    private TaskExecutor getTaskExecutor(String taskType) {
        TaskExecutor taskWorker = null;
        if (TaskType.ARES.getName().equals(taskType)) {
            taskWorker = taskExecutors.computeIfAbsent(TaskType.ARES.getBeanName(), v -> new AresExecutor());
        }
        if (taskWorker == null) {
            throw new AresException("task type not defined: " + taskType);
        }
        return taskWorker;
    }

    public void shutdown() {
        isRunning = false;
        if (taskExecutorPool != null) {
            taskExecutorPool.shutdownNow();
        }
    }
}
