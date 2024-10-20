package com.github.ares.spark.starter.execution;

import com.github.ares.core.starter.execution.RuntimeEnvironment;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;
import java.util.Properties;

public class SparkRuntimeEnvironment implements RuntimeEnvironment {
    private SparkConf sparkConf;

    private SparkSession sparkSession;

    private final Properties properties;

    private boolean enableHive = false;

    private String jobName;

    private SparkRuntimeEnvironment(Properties properties) {
        this.setEnableHive(checkIsContainHive(properties));
        this.properties = properties;
        this.initialize(properties);
    }

    public void setEnableHive(boolean enableHive) {
        this.enableHive = enableHive;
    }

    public Properties getConfig() {
        return this.properties;
    }

    public SparkRuntimeEnvironment prepare() {
        if (properties.containsKey("job.name")) {
            this.jobName = properties.getProperty("job.name");
        }
        sparkConf = createSparkConf();
        SparkSession.Builder builder = SparkSession.builder().config(sparkConf);
        if (enableHive) {
            builder.enableHiveSupport();
        }
        this.sparkSession = builder.getOrCreate();
        return this;
    }

    public SparkSession getSparkSession() {
        return this.sparkSession;
    }

    public SparkConf getSparkConf() {
        return this.sparkConf;
    }

    private SparkConf createSparkConf() {
        SparkConf sparkConfig = new SparkConf();
        if (StringUtils.isNotEmpty(jobName)) {
            sparkConfig.setAppName(jobName);
        }
        String sparkJars = (String) properties.remove("spark.jars");
        if (sparkJars != null) {
            String[] jars;
            if (sparkJars.contains(";")) {
                jars = sparkJars.split(";");
            } else {
                jars = sparkJars.split(",");
            }
            sparkConfig.setJars(jars);
        }
        properties.forEach((k, v) -> {
            if (((String) k).startsWith("spark.")) {
                sparkConfig.set((String) k, String.valueOf(v));
            }
        });

        return sparkConfig;
    }

    protected boolean checkIsContainHive(Properties properties) {
        /*List<? extends Config> sourceConfigList = config.getConfigList(PluginType.SOURCE.getType());
        for (Config c : sourceConfigList) {
            if (c.getString(PLUGIN_NAME_KEY).toLowerCase().contains("hive")) {
                return true;
            }
        }
        List<? extends Config> sinkConfigList = config.getConfigList(PluginType.SINK.getType());
        for (Config c : sinkConfigList) {
            if (c.getString(PLUGIN_NAME_KEY).toLowerCase().contains("hive")) {
                return true;
            }
        }*/
        return false;
    }

    public static SparkRuntimeEnvironment getInstance(Properties properties) {
//        if (INSTANCE == null) {
//            synchronized (SparkRuntimeEnvironment.class) {
//                if (INSTANCE == null) {
//                    INSTANCE = new SparkRuntimeEnvironment(properties);
//                }
//            }
//        }
//        return INSTANCE;
        return new SparkRuntimeEnvironment(properties);
    }
}
