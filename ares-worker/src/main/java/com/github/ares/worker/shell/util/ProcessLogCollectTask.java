package com.github.ares.worker.shell.util;

import com.github.ares.worker.utils.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ProcessLogCollectTask {

    protected final String taskInstanceLogFullPath;
    protected final Long taskInstanceId;
    protected volatile CountDownLatch countDownLatch;
    protected final TaskOutputParameterParser taskOutputParameterParser;

    private final Process process;

    public ProcessLogCollectTask(String taskInstanceLogFullPath,
                                 Long taskInstanceId,
                                 Process process) {
        this.taskInstanceLogFullPath = taskInstanceLogFullPath;
        this.taskInstanceId = taskInstanceId;
        this.countDownLatch = new CountDownLatch(1);
        this.taskOutputParameterParser = new TaskOutputParameterParser();
        this.process = process;
    }

    public void start() {
        ExecutorService singleThread = Executors.newSingleThreadExecutor();
        try {
            LogUtils.setTaskInstanceLogFullPathMDC(taskInstanceLogFullPath);
            LogUtils.setTaskInstanceIdMDC(taskInstanceId);
            singleThread.submit(this::doErrorCollect);
            doCollect();
        } finally {
            singleThread.shutdown();
            countDownLatch.countDown();
            LogUtils.removeTaskInstanceIdMDC();
            LogUtils.removeTaskInstanceLogFullPathMDC();
        }
    }

    public void doCollect() {
        try (
                InputStream inputStream = process.getInputStream();
                final InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                final BufferedReader inReader = new BufferedReader(inputStreamReader)) {
            String line;
            while ((line = inReader.readLine()) != null) {
                log.info("Task log: {}", line);
                parseOutputParam(line);
            }
        } catch (Exception e) {
            log.error("Handle process log error", e);
        }
    }

    public void doErrorCollect() {
        try (
                InputStream inputStream = process.getErrorStream();
                final InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                final BufferedReader inReader = new BufferedReader(inputStreamReader)) {
            String line;
            while ((line = inReader.readLine()) != null) {
                log.info("Task ERROR log: {}", line);
            }
        } catch (Exception e) {
            log.error("Handle process log error", e);
        }
    }

    protected void parseOutputParam(String log) {
        taskOutputParameterParser.appendParseLog(log);
    }

    public void waitFinish() throws InterruptedException {
        countDownLatch.await();
    }

    public Map<String, Object> getTaskOutputParams() {
        return taskOutputParameterParser.getOutputParams();
    }

    public List<Map<String, Object>> getTaskLastResult() {
        return taskOutputParameterParser.getLastResult();
    }

    public String getErrorMessage() {
        return taskOutputParameterParser.getErrorMessage();
    }
}
