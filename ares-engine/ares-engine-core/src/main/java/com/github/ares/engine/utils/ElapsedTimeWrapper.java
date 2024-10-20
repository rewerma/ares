package com.github.ares.engine.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.Consumer;

public class ElapsedTimeWrapper {
    protected static final Logger logger = LoggerFactory.getLogger("[SQLExecution]");

    public static void execute(String executionSQL, Execution execution) {
        long startTime = System.currentTimeMillis();

        execution.execute();

        long endTime = System.currentTimeMillis();
        BigDecimal elapsedTime = new BigDecimal(endTime - startTime)
                .divide(new BigDecimal(1000), 2, RoundingMode.HALF_UP);
        logger.info("Executed SQL: {}; elapsed time: {}s", executionSQL, elapsedTime);
    }

    public interface Execution {
        void execute();
    }
}
