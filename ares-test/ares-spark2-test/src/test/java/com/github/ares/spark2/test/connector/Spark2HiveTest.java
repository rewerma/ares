package com.github.ares.spark2.test.connector;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.github.ares.core.starter.command.Common;
import com.github.ares.spark.starter.AresSparkStarter;
import com.github.ares.test.spark.HiveTestUtils;
import com.github.ares.test.spark.Utils;
import java.nio.file.Path;
import java.util.Set;
import org.junit.Assume;
import org.junit.Test;

public class Spark2HiveTest {

    @Test
    public void embeddedHiveJobWithoutSparkHiveCatalog() {
        Assume.assumeTrue(HiveTestUtils.isHiveIntegrationEnabled());
        String[] args =
                new String[] {
                    "--master",
                    Utils.getSparkMaster(),
                    "--sql",
                    "../scripts/spark/connector/hive-test.sql",
                    "--conf",
                    "spark.jars="
                            + "../../ares-starter/ares-spark2-starter/target/ares-spark2-starter.jar"
                };
        AresSparkStarter.main(args);
    }

    @Test
    public void sparkSubmitJarPolicyShouldUseHadoopButNotThirdpartyHive() {
        assertTrue(Common.requiresHadoopThirdPartyConnector("hive"));
        assertTrue(Common.requiresHadoopThirdPartyConnector("FileHadoop"));

        Set<Path> thirdPartyJars =
                Common.getThirdPartyJars(
                        "/opt/ares/thirdparty/hive/hive-exec.jar;/opt/ares/lib/custom.jar");
        assertTrue(
                thirdPartyJars.stream().anyMatch(path -> path.toString().endsWith("custom.jar")));
        assertFalse(
                thirdPartyJars.stream()
                        .anyMatch(path -> path.toString().contains("thirdparty/hive")));
    }
}
