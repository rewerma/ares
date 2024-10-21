package com.github.ares.spark3.test.connector;

import com.github.ares.spark.starter.AresSparkStarter;
import com.github.ares.test.spark.Utils;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class Spark3FileLocalTest {
    @Test
    public void localFileTest() {
        String[] args = new String[]{
                "--master", Utils.getSparkMaster(),
                "--sql", "../scripts/spark/connector/file-local-test.sql",
                "--conf", "spark.jars="
                + "../../ares-starter/ares-spark3-starter/target/ares-spark3-starter.jar"
        };
        AresSparkStarter.main(args);
    }
}