package com.github.ares.web.utils;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import org.slf4j.MDC;

/**
 * task log filter
 */
public class TaskLogFilter extends Filter<ILoggingEvent> {

    @Override
    public FilterReply decide(ILoggingEvent event) {
        return MDC.get(LogUtils.TASK_INSTANCE_LOG_FULL_PATH_MDC_KEY) == null ? FilterReply.DENY : FilterReply.ACCEPT;
    }
}
