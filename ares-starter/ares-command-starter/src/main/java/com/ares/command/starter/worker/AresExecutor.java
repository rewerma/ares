package com.ares.command.starter.worker;

import com.ares.command.starter.TaskCallback;
import com.ares.command.starter.TaskExecutor;
import com.ares.command.starter.config.AresConfig;
import com.ares.command.starter.enums.EngineType;
import com.ares.command.starter.enums.StatusType;
import com.ares.command.starter.model.TaskContext;
import com.ares.command.starter.model.TaskRequest;
import com.ares.command.starter.model.TaskResponse;
import com.ares.command.starter.shell.ShellCommandExecutor;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.utils.JsonUtils;
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

import static com.github.ares.common.utils.Constants.EXECUTION_TASK_DIR;
import static com.github.ares.common.utils.Constants.LOG_EXT;
import static com.github.ares.common.utils.Constants.SCRIPT_SQL_FILE;

@Slf4j
public class AresExecutor implements TaskExecutor {
    private final Map<Long, ShellCommandExecutor> commandExecutors = new ConcurrentHashMap<>();

    private TaskCallback callback;

    private WorkerExecution workerExecution;

    private String rootPath;

    public void init() {
        workerExecution = new WorkerExecution();
        workerExecution.init();

        AresConfig aresConfig = AresConfig.getConfig();
        if (StringUtils.isNotBlank(aresConfig.getAresHome())) {
            if (!new File(aresConfig.getAresHome()).exists()) {
                throw new AresException("ares home path not found");
            }
            rootPath = aresConfig.getAresHome();
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
        AresConfig aresConfig = AresConfig.getConfig();
        String engineType = aresConfig.getEngineType();
        String sparkHome = aresConfig.getSparkHome();

        TaskResponse taskResponse = null;
        try {
            TaskRequest taskRequest = new TaskRequest();
            taskRequest.setTaskInstanceId(taskContext.getTaskInstanceId());
            String executePath = rootPath + File.separator + taskContext.getBatchCode()
                    + EXECUTION_TASK_DIR + taskContext.getTaskInstanceId();
            String logPath = executePath + File.separator + taskContext.getTaskInstanceId() + LOG_EXT;

            if (callback != null) {
                callback.running(logPath);
            }

            taskRequest.setExecutePath(executePath);
            taskRequest.setLogPath(logPath);

            if (StringUtils.isNotBlank(sparkHome)) {
                Map<String, String> environment = new LinkedHashMap<>();
                environment.put("SPARK_HOME", sparkHome);
                taskRequest.setEnvironments(environment);
            }

            ShellCommandExecutor executor = new ShellCommandExecutor(taskRequest);

            String scriptFile = executePath + File.separator + SCRIPT_SQL_FILE;
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
                envParams.put("--name", taskContext.getTaskName() + "-" + taskContext.getTaskInstanceId());
            }

            EngineType engineTypeEnum = EngineType.fromValue(engineType);
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
    public void registerCallback(TaskCallback callback) {
        this.callback = callback;
    }
}
