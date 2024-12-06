package com.github.ares.worker;

import com.github.ares.worker.model.TaskConfig;
import com.github.ares.worker.model.TaskContext;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

@Slf4j
public class WorkerExecution {

    private final LinkedBlockingQueue<TaskContext> taskExecutionQueue = new LinkedBlockingQueue<>();

    private ExecutorService taskExecutorPool;

    private volatile boolean isRunning = false;

    private Consumer<TaskContext> taskContextConsumer;

    public synchronized void init(TaskConfig taskConfig) {
        this.taskExecutorPool = Executors.newFixedThreadPool(taskConfig.getThreadPoolSize());
        if (!isRunning) {
            isRunning = true;
            run();
        }
    }

    public void registerTaskContextConsumer(Consumer<TaskContext> taskContextConsumer) {
        this.taskContextConsumer = taskContextConsumer;
    }

    private void run() {
        CompletableFuture.runAsync(() -> {
            while (isRunning) {
                try {
                    TaskContext taskContext = taskExecutionQueue.take();
                    // execute task
                    taskExecutorPool.submit(() -> {
                        taskContextConsumer.accept(taskContext);
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

    public void destroy() {
        isRunning = false;
        if (taskExecutorPool != null) {
            taskExecutorPool.shutdownNow();
        }
    }
}
