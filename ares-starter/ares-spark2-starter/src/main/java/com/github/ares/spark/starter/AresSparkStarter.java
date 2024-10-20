package com.github.ares.spark.starter;

import com.github.ares.api.common.EngineTypeVersion;
import com.github.ares.com.google.inject.AbstractModule;
import com.github.ares.com.google.inject.Singleton;
import com.github.ares.core.starter.command.Command;
import com.github.ares.core.starter.enums.EngineType;
import com.github.ares.core.starter.utils.CommandLineUtils;
import com.github.ares.engine.spark.config.SparkInjectorFactory;
import com.github.ares.engine.spark.core.SparkSinkExecutor;
import com.github.ares.spark.starter.args.SparkCommandArgs;
import com.github.ares.spark.starter.service.Spark2SinkExecutor;

public class AresSparkStarter {
    public static void main(String[] args) {
        SparkInjectorFactory.init(new AbstractModule(){
            @Override
            protected void configure() {
                bind(SparkSinkExecutor.class).to(Spark2SinkExecutor.class);
            }
        });

        SparkCommandArgs sparkCommandArgs =
                CommandLineUtils.parse(
                        args,
                        new SparkCommandArgs(),
                        EngineType.SPARK2.getStarterShellName(),
                        true);
        Command<?> command =  sparkCommandArgs.buildCommand(EngineTypeVersion.SPARK2);
        command.execute();
    }
}

