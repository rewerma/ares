package com.github.ares.spark2.test.function;

import com.github.ares.spark.starter.AresSparkStarter;
import com.github.ares.test.spark.Utils;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class Spark2FuncSystemTest {
    @Test
    public void sysFuncTest() {
        String[] args = new String[]{
                "--master", Utils.getSparkMaster(),
                "--sql", "../scripts/spark/function/spark2-system-function-test.sql",
                "--conf", "spark.jars="
                + "../../ares-starter/ares-spark2-starter/target/ares-spark2-starter.jar"
        };
        AresSparkStarter.main(args);
    }
}
