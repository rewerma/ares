package com.github.ares.web.utils;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.sift.AbstractDiscriminator;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

@Slf4j
public class TaskLogDiscriminator extends AbstractDiscriminator<ILoggingEvent> {

    private String key;

    private String logBase;

    @Override
    public String getDiscriminatingValue(ILoggingEvent event) {
        String taskInstanceLogPath = MDC.get(LogUtils.TASK_INSTANCE_LOG_FULL_PATH_MDC_KEY);
        if (taskInstanceLogPath == null) {
            log.error("The task instance log path is null, please check the logback configuration, log: {}", event);
        }
        return taskInstanceLogPath;
    }

    @Override
    public void start() {
        started = true;
    }

    @Override
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getLogBase() {
        return logBase;
    }

    public void setLogBase(String logBase) {
        this.logBase = logBase;
    }
}
