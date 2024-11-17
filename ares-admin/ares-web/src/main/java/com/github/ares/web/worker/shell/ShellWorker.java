package com.github.ares.web.worker.shell;

import com.github.ares.common.utils.JsonUtils;
import com.github.ares.web.dto.TaskContext;
import com.github.ares.web.dto.TaskRequest;
import com.github.ares.web.dto.TaskResponse;
import com.github.ares.web.enums.StatusType;
import com.github.ares.web.utils.ServiceException;
import com.github.ares.web.worker.Callback;
import com.github.ares.web.worker.TaskWorker;
import com.github.ares.web.worker.shell.util.ShellCommandExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

@Slf4j
@Component("shellWorker")
public class ShellWorker implements TaskWorker {

    private final Map<Long, ShellCommandExecutor> commandExecutors = new ConcurrentHashMap<>();

    private final LinkedBlockingQueue<TaskContext> taskExecutionQueue = new LinkedBlockingQueue<>();

    private final ExecutorService taskExecutorPool = Executors.newFixedThreadPool(50);

    private Callback callback;

    private volatile boolean isRunning = false;

    @PostConstruct
    public void init() {
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
                        executeTask(taskContext);
                    });

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, Executors.newSingleThreadExecutor());
    }

    private void executeTask(TaskContext taskContext) {
        TaskResponse taskResponse = null;
        String rootPath = "/Users/rewerma/Develop/ares-1.0";
        try {

            TaskRequest taskRequest = new TaskRequest();
            taskRequest.setTaskInstanceId(taskContext.getTaskInstanceId());
            String executePath = rootPath + "/task/" + taskContext.getTaskInstanceId();
            String logPath = executePath + File.separator + taskContext.getTaskInstanceId() + ".log";

            if (callback != null) {
                callback.running(logPath);
            }

            taskRequest.setExecutePath(executePath);
            taskRequest.setLogPath(logPath);
            ShellCommandExecutor executor = new ShellCommandExecutor(taskRequest);

            String scriptFile = executePath + File.separator + "script.sql";
            Path scriptPath = new File(scriptFile).toPath();
            if (!scriptPath.getParent().toFile().exists()) {
                scriptPath.getParent().toFile().mkdirs();
            }
            Files.createFile(scriptPath);
            Files.write(scriptPath, taskContext.getTaskContent().getBytes());

            StringBuilder execCommand = new StringBuilder();
            execCommand.append("cd ").append(rootPath).append("\n");
            execCommand.append("./bin/ares-local-starter.sh");
            execCommand.append(" --sql ");
            execCommand.append(scriptFile);
            Map<String, Object> envParams = null;
            if (StringUtils.isNotBlank(taskContext.getEnvParams())) {
                envParams = JsonUtils.parseObject(taskContext.getEnvParams(), Map.class);
            }
            if (envParams == null) {
                envParams = new LinkedHashMap<>();
            }
            if (!envParams.containsKey("--name")) {
                envParams.put("--name", taskContext.getTaskName() + "-" + taskContext.getTaskInstanceId());
            }
            envParams.forEach((k, v) -> execCommand.append(" ").append(k).append(" ").append(v).append(" "));

            commandExecutors.put(taskContext.getTaskInstanceId(), executor);
            taskResponse = executor.run(execCommand.toString());
            taskResponse.setLogPath(logPath);

            if (taskResponse.getStatus() == StatusType.FAILED) {
                return;
            }
            if (executor.isKilled()) {
                taskResponse.setStatus(StatusType.STOPPED);
            } else {
                taskResponse.setStatus(StatusType.SUCCESS);

                List<Map<String, Object>> resultJson = taskResponse.getLastResult();
                if (resultJson != null) {
                    Path resultFile = new File(executePath + File.separator + "result.json").toPath();
                    String result = JsonUtils.toJsonString(resultJson);
                    Files.createFile(resultFile);
                    Files.write(resultFile, result.getBytes());
                }
            }
        } catch (Exception e) {
            log.error("task execution error: {}", e.getMessage(), e);
            if (taskResponse == null) {
                taskResponse = new TaskResponse();
            }
            taskResponse.setStatus(StatusType.FAILED);
            taskResponse.setErrorMessage(e.getMessage());
        } finally {
            commandExecutors.remove(taskContext.getTaskInstanceId());
            if (callback != null) {
                callback.completed(taskResponse);
            }
        }
    }

    @Override
    public void start(TaskContext taskContext) {
        taskExecutionQueue.add(taskContext);
    }

    @Override
    public void stop(TaskContext taskContext) {
        ShellCommandExecutor executor = commandExecutors.get(taskContext.getTaskInstanceId());
        if (executor != null) {
            try {
                executor.cancelApplication();
            } catch (IOException e) {
                log.error("task execution stop error: {}", e.getMessage(), e);
            }
        } else {
            log.error("task instance has finished: {}", taskContext.getTaskContent());
        }
    }

    @Override
    public String getFullLog(TaskContext taskContext) {
        if (taskContext.getLogPath() == null) {
            return null;
        }
        try {
            return new String(Files.readAllBytes(new File(taskContext.getLogPath()).toPath()), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ServiceException("task log read error");
        }
    }

    @Override
    public void registerCallback(Callback callback) {
        this.callback = callback;
    }

    @PreDestroy
    public void destroy() {
        isRunning = false;
        taskExecutorPool.shutdownNow();
    }
}
