package com.github.ares.engine.core;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.parser.plan.LogicalProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public abstract class AbstractRootExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger("[ARES-LOGGER]");
    @Inject
    protected UdfManager udfManager;

    public void execute(LogicalProject baseBody) {
        try {
            executorManager.projectExecutor.execute(baseBody.getLogicalOperations());
        } catch (Exception e) {
            logger.error("[ERROR] Execution failed, caused by: {}", e.getMessage(), e);
            throw e;
        }
    }

    protected abstract List<Map<String, Object>> lastDataHandler(Object lastRes);
}
