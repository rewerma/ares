package com.github.ares.engine.core;

import static com.github.ares.common.utils.StringUtils.println;

import com.github.ares.engine.utils.JsonUtil;
import com.github.ares.parser.plan.LogicalProject;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRootExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    private static final Logger logger = LoggerFactory.getLogger("[ARES-LOGGER]");

    public void execute(LogicalProject baseBody) {
        try {
            Object lastRes =
                    executorManager.projectExecutor.execute(baseBody.getLogicalOperations());
            if (lastRes != null) {
                List<Map<String, Object>> lines = lastDataHandler(lastRes);
                String jsonStr = JsonUtil.getJsonMapper().writeValueAsString(lines);
                println("[LAST_RESULT]: " + jsonStr);
            }
        } catch (Exception e) {
            logger.error("[ERROR] Execution failed, caused by: {}", e.getMessage(), e);
            println("[ARES-FAILED] Execution failed, caused by: " + e.getMessage());
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Ares execution failed", e);
        }
    }

    protected abstract List<Map<String, Object>> lastDataHandler(Object lastRes);
}
