package com.github.ares.spark.starter.execution;

import com.github.ares.api.common.EngineTypeVersion;
import com.github.ares.common.exceptions.TaskExecuteException;
import com.github.ares.core.starter.execution.TaskExecution;
import com.github.ares.engine.spark.core.MainExecutor;
import com.github.ares.parser.hive.HiveTables;
import com.github.ares.parser.paimon.PaimonTables;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class SparkExecution implements TaskExecution {
    private final SparkRuntimeEnvironment sparkRuntimeEnvironment;

    private final Path sqlScript;
    private final Properties properties;
    private EngineTypeVersion engineTypeVersion;

    public SparkExecution(
            EngineTypeVersion engineTypeVersion, Path sqlScript, Properties properties) {
        this.engineTypeVersion = engineTypeVersion;
        this.sqlScript = sqlScript;
        this.properties = properties;
        if (scriptUsesHive(sqlScript) || scriptUsesPaimonHive(sqlScript)) {
            properties.setProperty(SparkRuntimeEnvironment.ENABLE_HIVE_KEY, "true");
        }
        if (scriptUsesPaimonFilesystem(sqlScript)) {
            appendSparkExtension(properties, PaimonTables.SPARK_EXTENSIONS_CLASS);
        }
        this.sparkRuntimeEnvironment = SparkRuntimeEnvironment.getInstance(properties);
    }

    private static boolean scriptUsesPaimonHive(Path sqlScript) {
        return PaimonTables.scriptUsesHiveMetastore(readScript(sqlScript));
    }

    private static boolean scriptUsesPaimonFilesystem(Path sqlScript) {
        return PaimonTables.scriptUsesFilesystem(readScript(sqlScript));
    }

    private static String readScript(Path sqlScript) {
        if (sqlScript == null || !Files.exists(sqlScript)) {
            return null;
        }
        try {
            return new String(Files.readAllBytes(sqlScript), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return null;
        }
    }

    private static void appendSparkExtension(Properties properties, String extension) {
        String key = "spark.sql.extensions";
        String current = properties.getProperty(key);
        if (current == null || current.trim().isEmpty()) {
            properties.setProperty(key, extension);
            return;
        }
        if (!current.contains(extension)) {
            properties.setProperty(key, current + "," + extension);
        }
    }

    private static boolean scriptUsesHive(Path sqlScript) {
        return HiveTables.scriptUsesHive(readScript(sqlScript));
    }

    @Override
    public void execute() throws TaskExecuteException {
        MainExecutor mainExecutor = MainExecutor.getInstance();
        try {
            mainExecutor.init(
                    engineTypeVersion,
                    sparkRuntimeEnvironment.getSparkSession(),
                    sqlScript,
                    properties);
            mainExecutor.run();
        } finally {
            mainExecutor.close();
        }
    }
}
