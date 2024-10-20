package com.github.ares.spark2.test;

import com.github.ares.spark.starter.AresSparkStarter;
import com.github.ares.test.spark.Utils;
import org.junit.Test;

public class Spark2MainStartTest {
    @Test
    public void testMain() {
        String[] args = new String[]{
                "--master", Utils.getSparkMaster(),
                "--sql", "../scripts/spark/main-start-test.sql",
                "--conf", "spark.jars="
                + "../../ares-starter/ares-spark2-starter/target/ares-spark2-starter.jar"
        };
        AresSparkStarter.main(args);
    }
}
