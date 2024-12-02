package com.github.ares.worker.shell;

import com.github.ares.com.google.common.collect.Maps;
import com.github.ares.common.enums.EngineType;
import com.github.ares.common.enums.StatusType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.worker.Callback;
import com.github.ares.worker.TaskWorker;
import com.github.ares.worker.WorkerExecution;
import com.github.ares.worker.model.TaskConfig;
import com.github.ares.worker.model.TaskContext;
import com.github.ares.worker.model.TaskRequest;
import com.github.ares.worker.model.TaskResponse;
import com.github.ares.worker.shell.util.ShellCommandExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.ares.common.utils.Constants.EXECUTION_LOGS_DIR;
import static com.github.ares.common.utils.Constants.EXECUTION_TASK_DIR;
import static com.github.ares.common.utils.Constants.LOG_EXT;
import static com.github.ares.common.utils.Constants.SCRIPT_SQL_FILE;
import static com.github.ares.common.utils.DateTimeUtils.getCurrentDateStr;
import static java.io.File.separator;

@Slf4j
public class AresWorker implements TaskWorker {
    private final Map<Long, ShellCommandExecutor> commandExecutors = new ConcurrentHashMap<>();

    private final Map<Long, Callback> callbackMap;

    private final WorkerExecution workerExecution;

    private final TaskConfig taskConfig;

    public AresWorker(TaskConfig taskConfig, WorkerExecution workerExecution) {
        this.taskConfig = taskConfig;
        this.workerExecution = workerExecution;
        this.callbackMap = Maps.newConcurrentMap();
        init();
    }

    private String rootPath;

    public void init() {
        if (StringUtils.isNotBlank(taskConfig.getAresHome())) {
            if (!new File(taskConfig.getAresHome()).exists()) {
                throw new AresException("ares home path not found");
            }
            rootPath = taskConfig.getAresHome();
        } else {
            File currentDir = new File("");
            File configDir = new File(currentDir.getAbsolutePath() + "/config");
            if (configDir.exists()) {
                rootPath = currentDir.getAbsolutePath();
            } else {
                throw new AresException("ares home path not found");
            }
        }
    }

    public void executeTask(TaskContext taskContext) {
        TaskResponse taskResponse = null;
        Callback callback = callbackMap.get(taskContext.getTaskInstanceId());
        try {
            TaskRequest taskRequest = new TaskRequest();
            taskRequest.setTaskInstanceId(taskContext.getTaskInstanceId());
            String executePath = rootPath + separator + EXECUTION_LOGS_DIR + separator + getCurrentDateStr() +
                    separator + taskContext.getBatchCode() + separator + taskContext.getTaskInstanceId();
            String logPath = executePath + separator + taskContext.getTaskInstanceId() + LOG_EXT;

            if (callback != null) {
                callback.running(logPath);
            }

            taskRequest.setExecutePath(executePath);
            taskRequest.setLogPath(logPath);

            String sparkHome = taskContext.getSparkHome();
            if (StringUtils.isBlank(sparkHome)) {
                sparkHome = taskConfig.getSparkHome();
            }
            if (StringUtils.isNotBlank(sparkHome)) {
                Map<String, String> environment = new LinkedHashMap<>();
                environment.put("SPARK_HOME", sparkHome);
                taskRequest.setEnvironments(environment);
            }

            ShellCommandExecutor executor = new ShellCommandExecutor(taskRequest);

            String scriptFile = executePath + separator + SCRIPT_SQL_FILE;
            Path scriptPath = new File(scriptFile).toPath();
            if (!scriptPath.getParent().toFile().exists()) {
                scriptPath.getParent().toFile().mkdirs();
            }
            Files.createFile(scriptPath);
            Files.write(scriptPath, taskContext.getTaskContent().getBytes());

            StringBuilder execCommand = new StringBuilder();
            execCommand.append("cd ").append(rootPath).append("\n");

            Map<String, Object> envParams = null;
            if (StringUtils.isNotBlank(taskContext.getEnvParams())) {
                envParams = JsonUtils.parseObject(taskContext.getEnvParams(), Map.class);
            }
            if (envParams == null) {
                envParams = new LinkedHashMap<>();
            }
            if (!envParams.containsKey("--name")) {
                if(StringUtils.isNotBlank(taskContext.getTaskName())) {
                    envParams.put("--name", taskContext.getTaskName() + "-" + taskContext.getTaskInstanceId());
                } else {
                    envParams.put("--name", "ARES-" + taskContext.getTaskInstanceId());
                }
            }

            String engineType = taskContext.getEngineType();
            if (StringUtils.isBlank(engineType)) {
                engineType = taskConfig.getEngineType();
            }
            EngineType engineTypeEnum;
            if (StringUtils.isBlank(engineType)) {
                engineTypeEnum = EngineType.LOCAL;
            } else {
                engineTypeEnum = EngineType.fromValue(engineType);
            }
            execCommand.append("./bin/").append(engineTypeEnum.getScriptFile());
            execCommand.append(" --sql ");
            execCommand.append(scriptFile);


            envParams.forEach((k, v) -> {
                if (k.startsWith("--")) {
                    execCommand.append(" ").append(k).append(" ").append(v).append(" ");
                }
            });

            commandExecutors.put(taskContext.getTaskInstanceId(), executor);


            taskResponse = executor.run(execCommand.toString());
            taskResponse.setLogPath(logPath);


            if (executor.isKilled()) {
                taskResponse.setStatus(StatusType.STOPPED);
            } else if (taskResponse.getStatus() != StatusType.FAILED) {
                taskResponse.setStatus(StatusType.SUCCESS);

                List<Map<String, Object>> resultJson = taskResponse.getLastResult();
                if (resultJson != null) {
                    Path resultFile = new File(executePath + separator + "result.json").toPath();
                    String result = JsonUtils.toJsonString(resultJson);
                    Files.createFile(resultFile);
                    Files.write(resultFile, result.getBytes());
                }
            }

            // write status to file
            Path resultFile = new File(executePath + separator + "result").toPath();
            Files.createFile(resultFile);
            Files.write(resultFile, taskResponse.getStatus().name().getBytes());

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
                callbackMap.remove(taskContext.getTaskInstanceId());
            }
        }
    }

    @Override
    public void start(TaskContext taskContext) {
        workerExecution.start(taskContext);
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
            throw new AresException("task log read error");
        }
    }

    @Override
    public void registerCallback(Long key, Callback callback) {
        callbackMap.put(key, callback);
    }
}
