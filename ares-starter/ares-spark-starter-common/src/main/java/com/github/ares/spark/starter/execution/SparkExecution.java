package com.github.ares.spark.starter.execution;

import com.github.ares.api.common.EngineTypeVersion;
import com.github.ares.common.exceptions.TaskExecuteException;
import com.github.ares.core.starter.execution.TaskExecution;
import com.github.ares.engine.spark.core.MainExecutor;

import java.nio.file.Path;
import java.util.Properties;

public class SparkExecution implements TaskExecution {
    private final SparkRuntimeEnvironment sparkRuntimeEnvironment;

    private final Path sqlScript;
    private final Properties properties;
    private EngineTypeVersion engineTypeVersion;

    public SparkExecution(EngineTypeVersion engineTypeVersion, Path sqlScript, Properties properties) {
        this.engineTypeVersion = engineTypeVersion;
        this.sparkRuntimeEnvironment = SparkRuntimeEnvironment.getInstance(properties);
        this.sqlScript = sqlScript;
        this.properties = properties;
    }

    @Override
    public void execute() throws TaskExecuteException {
        MainExecutor mainExecutor = MainExecutor.getInstance();
        try {
            mainExecutor.init(engineTypeVersion, sparkRuntimeEnvironment.getSparkSession(), sqlScript, properties);
            mainExecutor.run();
        } finally {
            mainExecutor.close();
        }
    }
}
