package com.github.ares.spark.starter.command;

import com.github.ares.api.common.EngineTypeVersion;
import com.github.ares.common.configuration.DeployMode;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.core.starter.command.Command;
import com.github.ares.core.starter.utils.FileUtils;
import com.github.ares.parser.PlParser;
import com.github.ares.parser.plan.LogicalProject;
import com.github.ares.parser.plan.LogicalSetConfig;
import com.github.ares.spark.starter.args.SparkCommandArgs;
import com.github.ares.spark.starter.execution.SparkExecution;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static com.github.ares.common.utils.Constants.DEPLOY_MODE_KEY;
import static com.github.ares.core.starter.utils.FileUtils.checkSqlScriptExist;

public class SparkTaskExecuteCommand implements Command<SparkCommandArgs> {

    private final SparkCommandArgs sparkCommandArgs;

    private final EngineTypeVersion engineTypeVersion;

    public SparkTaskExecuteCommand(SparkCommandArgs sparkCommandArgs, EngineTypeVersion engineTypeVersion) {
        this.sparkCommandArgs = sparkCommandArgs;
        this.engineTypeVersion = engineTypeVersion;
    }

    @Override
    public void execute() {
        Path sqlFile = FileUtils.getSqlPath(sparkCommandArgs);

        Properties jobConfig = new Properties();
        jobConfig.put(DEPLOY_MODE_KEY, sparkCommandArgs.getDeployMode());

        if (sparkCommandArgs.getMaster() != null) {
            jobConfig.put("spark.master", sparkCommandArgs.getMaster());
        }
        if (!sparkCommandArgs.getConfList().isEmpty()) {
            for (String conf : sparkCommandArgs.getConfList()) {
                String[] kv = conf.split("=");
                if (kv.length == 2) {
                    jobConfig.put(kv[0], kv[1]);
                }
            }
        }
        try {
            SparkExecution aresTaskExecution = new SparkExecution(engineTypeVersion, sqlFile, jobConfig);
            aresTaskExecution.execute();
        } catch (Exception e) {
            throw new AresException("Run Ares on spark failed", e);
        }
    }
}
