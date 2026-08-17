package com.github.ares.spark3.test.connector;

import com.github.ares.core.starter.command.Common;
import com.github.ares.spark.starter.AresSparkStarter;
import com.github.ares.test.spark.HiveTestUtils;
import com.github.ares.test.spark.Utils;
import org.junit.Assume;
import org.junit.Test;

import java.nio.file.Path;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Spark3HiveTest {

    @Test
    public void embeddedHive3JobWithoutSparkHiveCatalog() {
        Assume.assumeTrue(HiveTestUtils.isHiveIntegrationEnabled());
        String[] args =
                new String[] {
                    "--master", Utils.getSparkMaster(),
                    "--sql", "../scripts/spark/connector/hive3-test.sql",
                    "--conf", "spark.jars="
                            + "../../ares-starter/ares-spark3-starter/target/ares-spark3-starter.jar"
                };
        AresSparkStarter.main(args);
    }

    @Test
    public void sparkSubmitJarPolicyShouldUseHadoopButNotThirdpartyHive() {
        assertTrue(Common.requiresHadoopThirdPartyConnector("hive"));
        assertTrue(Common.requiresHadoopThirdPartyConnector("hive3"));
        assertTrue(Common.requiresHadoopThirdPartyConnector("FileHadoop"));

        Set<Path> thirdPartyJars =
                Common.getThirdPartyJars(
                        "/opt/ares/thirdparty/hive/hive-exec.jar;/opt/ares/thirdparty/hive3/hive-standalone-metastore.jar;/opt/ares/lib/custom.jar");
        assertTrue(
                thirdPartyJars.stream().anyMatch(path -> path.toString().endsWith("custom.jar")));
        assertFalse(
                thirdPartyJars.stream().anyMatch(path -> path.toString().contains("thirdparty/hive")));
    }
}
