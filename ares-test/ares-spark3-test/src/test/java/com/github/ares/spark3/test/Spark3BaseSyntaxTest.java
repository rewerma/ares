package com.github.ares.spark3.test;

import com.github.ares.spark.starter.AresSparkStarter;
import com.github.ares.test.spark.Utils;
import org.junit.Test;

public class Spark3BaseSyntaxTest {
    @Test
    public void sysFuncTest() {
        String[] args = new String[]{
                "--master", Utils.getSparkMaster(),
                "--sql", "../scripts/spark/base-syntax-test.sql",
                "--conf", "spark.jars="
                + "../../ares-starter/ares-spark3-starter/target/ares-spark3-starter.jar"
        };
        AresSparkStarter.main(args);
    }
}
