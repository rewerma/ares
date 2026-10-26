package com.github.ares.engine.core;

import com.github.ares.parser.config.PlProperties;
import java.util.Properties;

final class EngineTestSupport {
    private EngineTestSupport() {}

    static ExecutorManager managerWith(ExpressionExecutor expressionExecutor) {
        ExecutorManager manager = new ExecutorManager();
        TraceLogger logger = new TraceLogger();
        PlProperties properties = new PlProperties();
        Properties props = new Properties();
        props.setProperty("ares.pl.trace.enabled", "false");
        properties.init(props);
        logger.init(properties);
        manager.traceLogger = logger;
        manager.expressionExecutor = expressionExecutor;
        return manager;
    }
}
