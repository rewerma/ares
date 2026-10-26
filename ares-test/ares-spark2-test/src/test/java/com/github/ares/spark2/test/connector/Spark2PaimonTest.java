package com.github.ares.spark2.test.connector;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.github.ares.api.common.EngineType;
import com.github.ares.api.common.EngineTypeVersion;
import com.github.ares.api.common.ExecutionEngineType;
import com.github.ares.com.google.inject.Injector;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.common.utils.InjectorFactory;
import com.github.ares.core.starter.command.Common;
import com.github.ares.parser.PlParser;
import com.github.ares.parser.config.ParserInjectorFactory;
import com.github.ares.parser.datasource.PropertiesDataSourcePatcher;
import com.github.ares.parser.datasource.SourceConfigPatcherFactory;
import com.github.ares.parser.paimon.PaimonTables;
import com.github.ares.parser.plan.LogicalProject;
import com.github.ares.parser.utils.Constants;
import com.github.ares.spark.starter.AresSparkStarter;
import com.github.ares.spark.starter.PaimonStarterSupport;
import com.github.ares.test.spark.HiveTestUtils;
import com.github.ares.test.spark.Utils;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class Spark2PaimonTest {

    private static final String FILESYSTEM_SQL =
            "../scripts/spark/connector/paimon-filesystem-test.sql";
    private static final String HIVE_SQL = "../scripts/spark/connector/paimon-hive-test.sql";

    private PlParser parser;

    @Before
    public void initParser() {
        ExecutionEngineType.init(EngineType.SPARK, EngineTypeVersion.SPARK2);
        Injector injector = ParserInjectorFactory.create();
        InjectorFactory.init(injector);
        parser = injector.getInstance(PlParser.class);
        parser.init();
        SourceConfigPatcherFactory.register(
                Constants.DEFAULT_DATASOURCE_PATCHER,
                new PropertiesDataSourcePatcher(new Properties()));
    }

    @Test
    public void hiveCatalogOnSparkHive() {
        Assume.assumeTrue(HiveTestUtils.isHiveIntegrationEnabled());
        String[] args =
                new String[] {
                    "--master",
                    Utils.getSparkMaster(),
                    "--sql",
                    HIVE_SQL,
                    "--conf",
                    "spark.jars="
                            + "../../ares-starter/ares-spark2-starter/target/ares-spark2-starter.jar"
                };
        AresSparkStarter.main(args);
    }

    @Test
    public void filesystemCatalogRequiresSpark3() throws Exception {
        LogicalProject project = parser.parseToBaseBody(read(FILESYSTEM_SQL));
        assertTrue(PaimonTables.projectUsesFilesystem(project));
        try {
            PaimonStarterSupport.rejectFilesystemOnSpark2(project);
            fail("Paimon filesystem catalog requires Spark 3");
        } catch (ParseException ex) {
            assertTrue(ex.getMessage().contains("Spark 3"));
        }
    }

    @Test
    public void hiveCatalogDoesNotNeedHadoopThirdPartyConnector() throws Exception {
        LogicalProject project = parser.parseToBaseBody(read(HIVE_SQL));
        assertFalse(PaimonTables.projectUsesFilesystem(project));
        assertFalse(Common.requiresHadoopThirdPartyConnector("paimon"));
        PaimonStarterSupport.rejectFilesystemOnSpark2(project);
    }

    private static String read(String path) throws Exception {
        return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
    }
}
