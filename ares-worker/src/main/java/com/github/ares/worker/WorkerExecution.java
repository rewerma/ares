package com.github.ares.worker;

import com.github.ares.common.enums.TaskType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.worker.model.TaskConfig;
import com.github.ares.worker.model.TaskContext;
import com.github.ares.worker.shell.AresWorker;
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
//                        TaskWorker taskWorker = getTaskWorker(taskContext.getTaskType());
//                        taskWorker.executeTask(taskContext);
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

//    private TaskWorker getTaskWorker(String taskType) {
//        TaskWorker taskWorker = null;
//        // if (TaskType.ARES.getName().equals(taskType)) {
//        // }
//        taskWorker = new AresWorker(taskConfig);
//        // if (taskWorker == null) {
//        //     throw new AresException("task type not defined: " + taskType);
//        // }
//        return taskWorker;
//    }

    public void destroy() {
        isRunning = false;
        if (taskExecutorPool != null) {
            taskExecutorPool.shutdownNow();
        }
    }
}
