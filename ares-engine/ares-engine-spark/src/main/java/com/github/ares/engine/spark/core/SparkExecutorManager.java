package com.github.ares.engine.spark.core;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.parser.config.PlProperties;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

@Getter
public class SparkExecutorManager extends ExecutorManager implements Serializable {
    private static final long serialVersionUID = 1L;

    @Inject
    private SparkSessionManager sparkSessionManager;
    @Inject
    private SparkSinkExecutor sparkSinkExecutor;
    @Inject
    private SparkCommonExecutor sparkCommonExecutor;

    public void init(PlProperties plProperties, SparkSession sparkSession) {
        sparkSessionManager.init(sparkSession);
        super.init(plProperties);
    }
}

