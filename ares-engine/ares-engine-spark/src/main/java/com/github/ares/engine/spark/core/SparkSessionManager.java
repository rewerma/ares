package com.github.ares.engine.spark.core;

import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public class SparkSessionManager implements Serializable {
    private static final long serialVersionUID = -1L;

    private SparkSession sparkSession;

    public void init(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void close() {
        if (sparkSession != null) {
            sparkSession.close();
        }
    }
}
