package com.github.ares.spark2.test;

import com.github.ares.spark.starter.AresSparkStarter;
import com.github.ares.test.spark.Utils;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class Spark2ProcedureTest {
    @Test
    public void procedureTest() {
        String[] args = new String[]{
                "--master", Utils.getSparkMaster(),
                "--sql", "../scripts/spark/procedure-test.sql",
                "--conf", "spark.jars="
                + "../../ares-starter/ares-spark2-starter/target/ares-spark2-starter.jar;"
                + "../../ares-dist/target/ares-dist-test/ares-dist/connectors/connector-jdbc.jar;"
                + "../../ares-dist/target/ares-dist-test/ares-dist/lib/mysql-connector-j-8.3.0.jar",
        };
        AresSparkStarter.main(args);
    }
}
