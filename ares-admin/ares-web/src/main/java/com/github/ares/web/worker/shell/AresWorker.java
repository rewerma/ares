package com.github.ares.web.worker.shell;

import com.github.ares.common.utils.JsonUtils;
import com.github.ares.web.dto.TaskContext;
import com.github.ares.web.dto.TaskRequest;
import com.github.ares.web.dto.TaskResponse;
import com.github.ares.web.enums.EngineType;
import com.github.ares.web.enums.StatusType;
import com.github.ares.web.utils.ServiceException;
import com.github.ares.web.worker.Callback;
import com.github.ares.web.worker.TaskWorker;
import com.github.ares.web.worker.WorkerExecution;
import com.github.ares.web.worker.shell.util.ShellCommandExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.ares.web.utils.Constants.EXECUTION_TASK_DIR;
import static com.github.ares.web.utils.Constants.LOG_EXT;
import static com.github.ares.web.utils.Constants.SCRIPT_SQL_FILE;

@Slf4j
@Component("aresWorker")
public class AresWorker implements TaskWorker {
    private final Map<Long, ShellCommandExecutor> commandExecutors = new ConcurrentHashMap<>();

    private Callback callback;

    @Autowired
    private WorkerExecution workerExecution;

    @Value("${ares.ares-home:}")
    private String aresHome;

    @Value("${ares.engine-type:}")
    private String engineType;

    @Value("${ares.spark-home:}")
    private String sparkHome;

    private String rootPath;

    @PostConstruct
    public void init() {
        if (StringUtils.isNotBlank(aresHome)) {
            if (!new File(aresHome).exists()) {
                throw new ServiceException("ares home path not found");
            }
            rootPath = aresHome;
        } else {
            File currentDir = new File("");
            File configDir = new File(currentDir.getAbsolutePath() + "/config");
            if (configDir.exists()) {
                rootPath = currentDir.getAbsolutePath();
            } else {
                throw new ServiceException("ares home path not found");
            }
        }
    }

    public void executeTask(TaskContext taskContext) {
        TaskResponse taskResponse = null;
        try {
            TaskRequest taskRequest = new TaskRequest();
            taskRequest.setTaskInstanceId(taskContext.getTaskInstanceId());
            String executePath = rootPath + EXECUTION_TASK_DIR + taskContext.getTaskInstanceId();
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

            EngineType engineTypeEnum = EngineType.fromValue(this.engineType);
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
            throw new ServiceException("task log read error");
        }
    }

    @Override
    public void registerCallback(Callback callback) {
        this.callback = callback;
    }
}
