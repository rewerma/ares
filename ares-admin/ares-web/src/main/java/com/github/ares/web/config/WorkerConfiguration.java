package com.github.ares.web.config;

import com.github.ares.worker.TaskWorker;
import com.github.ares.worker.WorkerExecution;
import com.github.ares.worker.model.TaskConfig;
import com.github.ares.worker.model.TaskContext;
import com.github.ares.worker.shell.AresWorker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class WorkerConfiguration {
    @Value("${ares.ares-home:}")
    private String aresHome;

    @Value("${ares.engine-type:}")
    private String engineType;

    @Value("${ares.spark-home:}")
    private String sparkHome;

    @Value("${ares.worker-thread-pool-size:50}")
    private int threadPoolSize;

    @Bean
    public TaskConfig taskConfig() {
        TaskConfig taskConfig = new TaskConfig();
        taskConfig.setAresHome(aresHome);
        taskConfig.setEngineType(engineType);
        taskConfig.setSparkHome(sparkHome);
        taskConfig.setThreadPoolSize(threadPoolSize);
        return taskConfig;
    }

    @Bean
    public WorkerExecution workerExecution(TaskConfig taskConfig) {
        WorkerExecution workerExecution = new WorkerExecution();
        workerExecution.init(taskConfig);
        return workerExecution;
    }

    @Bean
    public TaskWorker taskWorker(TaskConfig taskConfig, WorkerExecution workerExecution) {
        TaskWorker taskWorker = new AresWorker(taskConfig, workerExecution);
        workerExecution.registerTaskContextConsumer(taskWorker::executeTask);
        return taskWorker;
    }
}
