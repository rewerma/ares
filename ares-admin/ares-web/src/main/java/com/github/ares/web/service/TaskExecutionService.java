package com.github.ares.web.service;

import com.github.ares.common.utils.JsonUtils;
import com.github.ares.web.dto.TaskContext;
import com.github.ares.web.dto.TaskRequest;
import com.github.ares.web.dto.TaskResponse;
import com.github.ares.web.entity.BaseModel;
import com.github.ares.web.entity.Datasource;
import com.github.ares.web.entity.TaskDefinition;
import com.github.ares.web.entity.TaskInstance;
import com.github.ares.web.shell.ShellCommandExecutor;
import com.github.ares.web.utils.CodeGenerator;
import com.github.ares.web.utils.ServiceException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Service
public class TaskExecutionService {

    private final Map<Long, ShellCommandExecutor> commandExecutors = new ConcurrentHashMap<>();

    private final LinkedBlockingQueue<TaskContext> taskExecutionQueue = new LinkedBlockingQueue<>();

    private final ExecutorService taskExecutorPool = Executors.newCachedThreadPool();

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

//    public static void main(String[] args) {
//        TaskExecutionService service = new TaskExecutionService();
//        TaskContext taskContext = new TaskContext();
//        taskContext.setTaskInstanceId(123345L);
//        taskContext.setTaskContent("select 1 as test;");
//
//        service.executeTask(taskContext);
//    }

    public void executeTask(TaskContext taskContext) {
        TaskInstance taskInstance = new TaskInstance();
        taskInstance.setId(taskContext.getTaskInstanceId());
        String rootPath = "/Users/rewerma/Develop/ares-1.0";
        try {
            TaskRequest taskRequest = new TaskRequest();
            taskRequest.setTaskInstanceId(taskContext.getTaskInstanceId());
            String executePath = rootPath + "/task/" + taskContext.getTaskInstanceId();
            String logPath = executePath + File.separator + taskContext.getTaskInstanceId() + ".log";
            taskInstance.setLogPath(logPath);
            taskInstance.setStatus(2);
            taskInstance.update("logPath", "status");

            taskRequest.setExecutePath(executePath);
            taskRequest.setLogPath(logPath);
            ShellCommandExecutor executor = new ShellCommandExecutor(taskRequest);

            // TODO write to Ares script file
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
            TaskResponse taskResponse = executor.run(execCommand.toString());


            Map<String, Object> outputParams = taskResponse.getOutputParams();
            if (outputParams != null) {
                taskInstance.setOutParams(JsonUtils.toJsonString(outputParams));
            }
            List<Map<String, Object>> resultJson = taskResponse.getLastResult();
            if (resultJson != null) {
                Path resultFile = new File(executePath + File.separator + "result.json").toPath();
                String result = JsonUtils.toJsonString(resultJson);
                Files.createFile(resultFile);
                Files.write(resultFile, result.getBytes());
                taskInstance.setExeResult(result);
            }


            if (taskResponse.getErrorMessage() != null) {
                taskInstance.setStatus(4);
                taskInstance.setExeResult(taskResponse.getErrorMessage());
                taskInstance.setEndTime(LocalDateTime.now());
                taskInstance.update("status", "outParams", "endTime", "exeResult");
            } else {
                TaskInstance taskInstanceExist = BaseModel.query(TaskInstance.class).where().eq("id", taskContext.getTaskInstanceId()).findOne();
                if (taskInstanceExist != null && taskInstanceExist.getStatus() == 2) {
                    taskInstance.setStatus(3);
                    taskInstance.setEndTime(LocalDateTime.now());
                    taskInstance.update("status", "outParams", "endTime", "exeResult");
                }
            }
        } catch (Exception e) {
            log.error("task execution error: {}", e.getMessage(), e);
            taskInstance.setStatus(4);
            taskInstance.setEndTime(LocalDateTime.now());
            taskInstance.setExeResult(e.getMessage());
            taskInstance.update("status", "exeResult");
        } finally {
            commandExecutors.remove(taskContext.getTaskInstanceId());
        }
    }

    @PreDestroy
    public void destroy() {
        isRunning = false;
    }

    public Long start(String code) {
        TaskDefinition taskDefinition = BaseModel.query(TaskDefinition.class).where().eq("code", code).findOne();
        if (taskDefinition == null) {
            throw new ServiceException("task definition not found");
        }
        // generate task instance
        TaskInstance taskInstance = new TaskInstance();
        taskInstance.setBatchCode(CodeGenerator.generateCode());
        taskInstance.setTaskCode(taskDefinition.getCode());
        taskInstance.setTaskName(taskDefinition.getName());
        taskInstance.setStartTime(LocalDateTime.now());
        taskInstance.setStatus(1);
        taskInstance.save();

        TaskContext taskContext = new TaskContext();
        taskContext.setTaskCode(taskDefinition.getCode());
        taskContext.setTaskName(taskDefinition.getName());
        taskContext.setTaskInstanceId(taskInstance.getId());
        String taskContent = handleDatasource(taskDefinition.getDsCode(), taskDefinition.getTaskContent());
        taskContext.setTaskContent(taskContent);
        taskContext.setEnvParams(taskDefinition.getEnvParams());
        taskContext.setInParams(taskDefinition.getInParams());
        taskContext.setOutParams(taskDefinition.getOutParams());

        taskExecutionQueue.add(taskContext);

        return taskInstance.getId();
    }

    private String handleDatasource(String dsCode, String taskContent) {
        if(StringUtils.isBlank(dsCode)) {
            return taskContent;
        }
        String[] dsCodes = dsCode.split(",");
        StringBuilder dsContent = new StringBuilder();
        for(String code : dsCodes) {
            Datasource datasource = BaseModel.query(Datasource.class).where().eq("code", code).findOne();
            if (datasource == null) {
                continue;
            }
            Map<String, Object> params = JsonUtils.parseObject(datasource.getParams(), Map.class);
            if (params == null) {
                continue;
            }
            params.forEach((k, v) -> dsContent.append("SET datasource.").append(datasource.getName())
                    .append(".").append(k).append("=").append(v).append(";\n"));
            dsContent.append("\n");
        }

        return dsContent + taskContent;
    }

    public void stop(Long taskInstanceId) {
        ShellCommandExecutor executor = commandExecutors.get(taskInstanceId);
        if (executor != null) {
            TaskInstance taskInstance = new TaskInstance();
            taskInstance.setId(taskInstanceId);
            taskInstance.setStatus(5);
            taskInstance.setEndTime(LocalDateTime.now());
            taskInstance.update("status", "endTime");
            try {
                executor.cancelApplication();
            } catch (IOException e) {
                log.error("task execution stop error: {}", e.getMessage(), e);
            }
        } else {
            log.error("task instance has finished: {}", taskInstanceId);
        }
    }

    public String getFullLog(Long taskInstanceId) {
        TaskInstance taskInstance = BaseModel.query(TaskInstance.class).where().eq("id", taskInstanceId).findOne();
        if (taskInstance == null) {
            throw new ServiceException("task instance not found");
        }
        if (taskInstance.getLogPath() == null) {
            return null;
        }
        try {
            return new String(Files.readAllBytes(new File(taskInstance.getLogPath()).toPath()), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ServiceException("task log read error");
        }
    }
}
