package com.github.ares.web.service;

import com.github.ares.common.enums.StatusType;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.web.entity.BaseModel;
import com.github.ares.web.entity.Datasource;
import com.github.ares.web.entity.TaskDefinition;
import com.github.ares.web.entity.TaskInstance;
import com.github.ares.web.utils.CodeGenerator;
import com.github.ares.web.utils.NetUtils;
import com.github.ares.web.utils.ServiceException;
import com.github.ares.worker.TaskWorker;
import com.github.ares.worker.model.TaskContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;

@Slf4j
@Service
public class TaskExecutionService {
    @Value("${server.port:8080}")
    private Integer serverPort;

    @Autowired
    private TaskWorker taskWorker;

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
        taskInstance.setStatus(StatusType.READY.getValue());
        taskInstance.setExecutorHost(NetUtils.getHost() + ":" + serverPort);
        taskInstance.save();

        TaskContext taskContext = new TaskContext();
        taskContext.setTaskCode(taskDefinition.getCode());
        taskContext.setTaskType(taskDefinition.getTaskType());
        taskContext.setTaskName(taskDefinition.getName());
        taskContext.setTaskInstanceId(taskInstance.getId());
        String taskContent = handleDatasource(taskDefinition.getDsCode(), taskDefinition.getTaskContent());
        taskContext.setTaskContent(taskContent);
        taskContext.setEnvParams(taskDefinition.getEnvParams());
        taskContext.setInParams(taskDefinition.getInParams());
        taskContext.setOutParams(taskDefinition.getOutParams());

        taskWorker.registerCallback(taskInstance.getId(),
                new CallbackHandler(taskDefinition, taskInstance));

        taskInstance.setStatus(StatusType.SUBMIT.getValue());
        taskInstance.update("status");

        taskWorker.start(taskContext);

        return taskInstance.getId();
    }

    private String handleDatasource(String dsCode, String taskContent) {
        if (StringUtils.isBlank(dsCode)) {
            return taskContent;
        }
        String[] dsCodes = dsCode.split(",");
        StringBuilder dsContent = new StringBuilder();
        for (String code : dsCodes) {
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
        TaskInstance taskInstance = BaseModel.query(TaskInstance.class).where().eq("id", taskInstanceId).findOne();
        if (taskInstance == null) {
            throw new ServiceException("task instance not found");
        }
//        if (StringUtils.isBlank(taskInstance.getExecutorHost()) || taskInstance.getExecutorHost().startsWith(NetUtils.getHost())) {
        TaskContext taskContext = new TaskContext();
        taskContext.setTaskInstanceId(taskInstanceId);
        taskWorker.stop(taskContext);
//        } else {
//            // 创建 HttpClient 实例
//            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
//                String url = "https://"+taskInstance.getExecutorHost()+"/task/execution/"+taskInstanceId+"/stop";
//                HttpPost postRequest = new HttpPost(url);
//                postRequest.setHeader("Content-Type", "application/json");
//
////                // 设置请求体
////                String jsonBody = "{ \"key\": \"value\" }";
////                StringEntity entity = new StringEntity(jsonBody);
////                postRequest.setEntity(entity);
//
//                try (CloseableHttpResponse response = httpClient.execute(postRequest)) {
//                    // 打印响应状态码
//                    System.out.println("Status Code: " + response.getStatusLine());
//
//                    // 打印响应体
//                    String responseBody = EntityUtils.toString(response.getEntity());
//                    System.out.println("Response Body: " + responseBody);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//    }
    }

    public String getFullLog(Long taskInstanceId) {
        TaskInstance taskInstance = BaseModel.query(TaskInstance.class).where().eq("id", taskInstanceId).findOne();
        if (taskInstance == null) {
            throw new ServiceException("task instance not found");
        }
        TaskContext taskContext = new TaskContext();
        taskContext.setTaskInstanceId(taskInstanceId);
        taskContext.setLogPath(taskInstance.getLogPath());
        return taskWorker.getFullLog(taskContext);
    }
}
