package com.github.ares.engine.spark.core;

import java.io.Serializable;
import org.apache.spark.sql.SparkSession;

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
