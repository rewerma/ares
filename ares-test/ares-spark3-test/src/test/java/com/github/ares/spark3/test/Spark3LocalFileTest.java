package com.github.ares.spark3.test;

import com.github.ares.spark.starter.AresSparkStarter;
import com.github.ares.test.spark.Utils;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class Spark3LocalFileTest {
    @Test
    public void localFileTest() {
        String[] args = new String[]{
                "--master", Utils.getSparkMaster(),
                "--sql", "../scripts/spark/local-file-sink-test.sql",
                "--conf", "spark.jars="
                + "../../ares-starter/ares-spark3-starter/target/ares-spark3-starter.jar"
        };
        AresSparkStarter.main(args);
    }
}
