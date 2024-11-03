package com.github.ares.engine.core;

import com.github.ares.engine.utils.JsonUtil;
import com.github.ares.parser.plan.LogicalProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.github.ares.common.utils.StringUtils.println;

public abstract class AbstractRootExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    private static final Logger logger = LoggerFactory.getLogger("[ARES-LOGGER]");

    public void execute(LogicalProject baseBody) {
        try {
            Object lastRes = executorManager.projectExecutor.execute(baseBody.getLogicalOperations());
            if (lastRes != null) {
                List<Map<String, Object>> lines = lastDataHandler(lastRes);
                String jsonStr = JsonUtil.getJsonMapper().writeValueAsString(lines);
                println("[LAST_RESULT]: " + jsonStr);
            }
        } catch (Exception e) {
            logger.error("[ERROR] Execution failed, caused by: {}", e.getMessage(), e);
            println("[ARES-FAILED] Execution failed, caused by: " + e.getMessage());
        }
    }

    protected abstract List<Map<String, Object>> lastDataHandler(Object lastRes);
}
