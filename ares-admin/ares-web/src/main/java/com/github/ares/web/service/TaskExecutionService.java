package com.github.ares.web.service;

import com.github.ares.common.enums.StatusType;
import com.github.ares.common.enums.TaskType;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.parser.PlParser;
import com.github.ares.web.config.PLParserConfig;
import com.github.ares.web.dto.TaskExecutionDto;
import com.github.ares.web.entity.BaseModel;
import com.github.ares.web.entity.Datasource;
import com.github.ares.web.entity.TaskDefinition;
import com.github.ares.web.entity.TaskInstance;
import com.github.ares.web.utils.NetUtils;
import com.github.ares.web.utils.Result;
import com.github.ares.web.utils.ServiceException;
import com.github.ares.worker.TaskWorker;
import com.github.ares.worker.WorkerExecution;
import com.github.ares.worker.model.TaskContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class TaskExecutionService {
    @Value("${server.port:8080}")
    private Integer serverPort;

    @Autowired
    private WorkerExecution workerExecution;

    @Autowired
    private TaskWorker taskWorker;

    public Long start(String code, TaskExecutionDto taskExecutionDto) {
        TaskDefinition taskDefinition = BaseModel.query(TaskDefinition.class).where().eq("code", code).findOne();
        if (taskDefinition == null) {
            throw new ServiceException("task definition not found");
        }
        return start(taskDefinition, taskExecutionDto);
    }

    public Long start(TaskExecutionDto taskExecutionDto) {
        TaskDefinition taskDefinition = new TaskDefinition();
        taskDefinition.setName("test-exe");
        taskDefinition.setTaskType(TaskType.ARES.getName());
        taskDefinition.setInParams(taskExecutionDto.getInParams());
        taskDefinition.setTaskContent(taskExecutionDto.getTaskContent());
        return start(taskDefinition, taskExecutionDto);
    }

    public Long start(TaskDefinition taskDefinition, TaskExecutionDto taskExecutionDto) {
        // generate task instance
        TaskInstance taskInstance = new TaskInstance();
        taskInstance.setBatchCode(taskExecutionDto.getBatchCode() == null ? "0" : taskExecutionDto.getBatchCode());
        taskInstance.setTaskCode(taskDefinition.getCode());
        taskInstance.setTaskName(taskDefinition.getName());
        taskInstance.setStartTime(LocalDateTime.now());
        taskInstance.setStatus(StatusType.READY.getValue());
        taskInstance.setExecutorHost(NetUtils.getHost() + ":" + serverPort);
        taskInstance.save();

        try {
            TaskContext taskContext = new TaskContext();
            if (taskExecutionDto.getBatchCode() != null) {
                taskContext.setBatchCode(taskExecutionDto.getBatchCode());
            }
            taskContext.setTaskType(taskDefinition.getTaskType());
            taskContext.setTaskName(taskDefinition.getName());
            taskContext.setTaskInstanceId(taskInstance.getId());

            String taskContent = taskDefinition.getTaskContent();

            if (taskDefinition.getInParams() != null) {
                Map<String, Object> defInParams = JsonUtils.toMap2(taskDefinition.getInParams());
                if (taskExecutionDto.getInParams() != null) {
                    Map<String, Object> inParams = JsonUtils.toMap2(taskExecutionDto.getInParams());
                    inParams.forEach((k, v) -> {
                        if (defInParams.containsKey(k)) {
                            defInParams.put(k, v);
                        }
                    });
                }
                for (Map.Entry<String, Object> entry : defInParams.entrySet()) {
                    String key = "${" + entry.getKey() + "}";
                    String value = entry.getValue() == null ? "" : entry.getValue().toString();
                    taskContent = taskContent.replace(key, value);
                }
                taskInstance.setInParams(JsonUtils.toJsonString(defInParams));
            }

            PlParser plParser = PLParserConfig.getPlParser();
            // parse dataSources of task content
            List<String> usedDataSources = plParser.parseDataSources(taskContent);
            taskContent = handleDatasource(usedDataSources, taskContent);

            taskContext.setTaskContent(taskContent);
            taskContext.setEnvParams(taskDefinition.getEnvParams());

            taskWorker.registerCallback(taskInstance.getId(),
                    new CallbackHandler(taskDefinition, taskInstance));

            taskInstance.setStatus(StatusType.SUBMIT.getValue());
            taskInstance.update("status", "inParams");

            taskWorker.start(taskContext);

            return taskInstance.getId();
        } catch (Exception e) {
            taskInstance.setStatus(StatusType.FAILED.getValue());
            taskInstance.setExeResult(e.getMessage());
            taskInstance.setEndTime(LocalDateTime.now());
            taskInstance.update("status", "exeResult", "endTime");
            throw new ServiceException(e);
        }
    }

    private String handleDatasource(List<String> usedDataSources, String taskContent) {
        if (usedDataSources == null || usedDataSources.isEmpty()) {
            return taskContent;
        }
        StringBuilder dsContent = new StringBuilder();
        for (String dsName : usedDataSources) {
            Datasource datasource = BaseModel.query(Datasource.class).where().eq("name", dsName).findOne();
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
        TaskInstance taskInstance = BaseModel.query(TaskInstance.class).where().eq("id", taskInstanceId).findOne();
        if (taskInstance == null) {
            throw new ServiceException("task instance not found");
        }
        if (StringUtils.isBlank(taskInstance.getExecutorHost()) || taskInstance.getExecutorHost().startsWith(NetUtils.getHost())) {
            TaskContext taskContext = new TaskContext();
            taskContext.setTaskInstanceId(taskInstanceId);
            taskWorker.stop(taskContext);
        } else {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                String url = "http://" + taskInstance.getExecutorHost() + "/ares/task/execution/" + taskInstanceId + "/stop";
                HttpPost postRequest = new HttpPost(url);
                postRequest.setHeader("Content-Type", "application/json");

                try (CloseableHttpResponse response = httpClient.execute(postRequest)) {
                    // System.out.println("Status Code: " + response.getStatusLine());
                    String responseBody = EntityUtils.toString(response.getEntity());
                    if (StringUtils.isBlank(responseBody)) {
                        throw new ServiceException("stop task failed from executor host: " + taskInstance.getExecutorHost());
                    }
                    Result result = JsonUtils.parseObject(responseBody, Result.class);
                    if (result.getCode() == null || result.getCode() != 200) {
                        throw new ServiceException("stop task failed from executor host: " + taskInstance.getExecutorHost());
                    }
                }
            } catch (ServiceException e) {
                throw e;
            } catch (Exception e) {
                throw new ServiceException(e);
            }
        }
    }

    public String getFullLog(Long taskInstanceId) {
        TaskInstance taskInstance = BaseModel.query(TaskInstance.class).where().eq("id", taskInstanceId).findOne();
        if (taskInstance == null) {
            throw new ServiceException("task instance not found");
        }
        if (StringUtils.isBlank(taskInstance.getExecutorHost()) || taskInstance.getExecutorHost().startsWith(NetUtils.getHost())) {
            TaskContext taskContext = new TaskContext();
            taskContext.setTaskInstanceId(taskInstanceId);
            taskContext.setLogPath(taskInstance.getLogPath());
            return taskWorker.getFullLog(taskContext);
        } else {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                String url = "http://" + taskInstance.getExecutorHost() + "/ares/task/execution/" + taskInstanceId + "/log";
                HttpGet getRequest = new HttpGet(url);
                getRequest.setHeader("Content-Type", "application/json");

                try (CloseableHttpResponse response = httpClient.execute(getRequest)) {
                    String responseBody = EntityUtils.toString(response.getEntity());
                    if (StringUtils.isBlank(responseBody)) {
                        throw new ServiceException("get log failed from executor host: " + taskInstance.getExecutorHost());
                    }
                    Result result = JsonUtils.parseObject(responseBody, Result.class);
                    return (String) result.getData();
                }
            } catch (ServiceException e) {
                throw e;
            } catch (Exception e) {
                throw new ServiceException(e);
            }
        }
    }

    @PreDestroy
    public void destroy() {
        workerExecution.destroy();
    }
}
