package com.github.ares.web.worker;

import com.github.ares.web.dto.TaskContext;
import com.github.ares.web.enums.TaskType;
import com.github.ares.web.utils.ServiceException;
import com.github.ares.web.utils.SpringContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Component
public class WorkerExecution {

    private final LinkedBlockingQueue<TaskContext> taskExecutionQueue = new LinkedBlockingQueue<>();

    private ExecutorService taskExecutorPool;

    private volatile boolean isRunning = false;

    @Value("${ares.worker-thread-pool-size:50}")
    private int threadPoolSize;

    @PostConstruct
    public void init() {
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
                        TaskWorker taskWorker = getTaskWorker(taskContext.getTaskType());
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

    private TaskWorker getTaskWorker(String taskType) {
        TaskWorker taskWorker = null;
        if (TaskType.ARES.getName().equals(taskType)) {
            taskWorker = SpringContext.getBean(TaskType.ARES.getBeanName());
        }
        if (taskWorker == null) {
            throw new ServiceException("task type not defined: " + taskType);
        }
        return taskWorker;
    }

    @PreDestroy
    public void destroy() {
        isRunning = false;
        if (taskExecutorPool != null) {
            taskExecutorPool.shutdownNow();
        }
    }
}
