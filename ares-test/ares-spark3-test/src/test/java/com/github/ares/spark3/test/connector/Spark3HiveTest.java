package com.github.ares.spark3.test.connector;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.github.ares.core.starter.command.Common;
import com.github.ares.spark.starter.AresSparkStarter;
import com.github.ares.test.spark.HiveTestUtils;
import com.github.ares.test.spark.Utils;
import org.junit.Assume;
import org.junit.Test;

public class Spark3HiveTest {

    @Test
    public void hiveViewOnSparkHiveCatalog() {
        Assume.assumeTrue(HiveTestUtils.isHiveIntegrationEnabled());
        String[] args =
                new String[] {
                    "--master",
                    Utils.getSparkMaster(),
                    "--sql",
                    "../scripts/spark/connector/hive3-test.sql",
                    "--conf",
                    "spark.jars="
                            + "../../ares-starter/ares-spark3-starter/target/ares-spark3-starter.jar"
                };
        AresSparkStarter.main(args);
    }

    @Test
    public void hiveDoesNotNeedHadoopThirdPartyConnector() {
        assertFalse(Common.requiresHadoopThirdPartyConnector("hive"));
        assertFalse(Common.requiresHadoopThirdPartyConnector("hive3"));
        assertTrue(Common.requiresHadoopThirdPartyConnector("FileHadoop"));
    }
}
