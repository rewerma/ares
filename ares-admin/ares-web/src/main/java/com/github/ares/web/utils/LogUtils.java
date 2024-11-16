package com.github.ares.web.utils;

import lombok.AllArgsConstructor;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;


@Slf4j
@UtilityClass
public class LogUtils {
    private static final String TASK_INSTANCE_ID_MDC_KEY = "taskInstanceId";

    public static final String TASK_INSTANCE_LOG_FULL_PATH_MDC_KEY = "taskInstanceLogFullPath";

    public static MDCAutoClosableContext setTaskInstanceLogFullPathMDC(String taskInstanceLogFullPath) {
        MDC.put(TASK_INSTANCE_LOG_FULL_PATH_MDC_KEY, taskInstanceLogFullPath);
        return new MDCAutoClosableContext(LogUtils::removeTaskInstanceLogFullPathMDC);
    }

    public static void removeTaskInstanceLogFullPathMDC() {
        MDC.remove(TASK_INSTANCE_LOG_FULL_PATH_MDC_KEY);
    }
    public static void setTaskInstanceIdMDC(Long taskInstanceId) {
        MDC.put(TASK_INSTANCE_ID_MDC_KEY, String.valueOf(taskInstanceId));
    }

    public static void removeTaskInstanceIdMDC() {
        MDC.remove(TASK_INSTANCE_ID_MDC_KEY);
    }

    @AllArgsConstructor
    public static class MDCAutoClosableContext implements AutoCloseable {

        private final Runnable closeAction;

        @Override
        public void close() {
            closeAction.run();
        }
    }
}
