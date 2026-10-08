package com.github.ares.spark3.test.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.github.ares.api.common.EngineType;
import com.github.ares.api.common.EngineTypeVersion;
import com.github.ares.api.common.ExecutionEngineType;
import com.github.ares.com.google.inject.Injector;
import com.github.ares.common.utils.InjectorFactory;
import com.github.ares.core.starter.command.Common;
import com.github.ares.parser.PlParser;
import com.github.ares.parser.config.ParserInjectorFactory;
import com.github.ares.parser.datasource.PropertiesDataSourcePatcher;
import com.github.ares.parser.datasource.SourceConfigPatcherFactory;
import com.github.ares.parser.paimon.PaimonDml;
import com.github.ares.parser.paimon.PaimonTables;
import com.github.ares.parser.plan.LogicalAnonymousBody;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalDeleteSelectSQL;
import com.github.ares.parser.plan.LogicalMergeIntoSQL;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalProject;
import com.github.ares.parser.plan.LogicalUpdateSelectSQL;
import com.github.ares.parser.utils.Constants;
import com.github.ares.spark.starter.AresSparkStarter;
import com.github.ares.test.spark.HiveTestUtils;
import com.github.ares.test.spark.Utils;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class Spark3PaimonTest {

    private static final String FILESYSTEM_SQL =
            "../scripts/spark/connector/paimon-filesystem-test.sql";
    private static final String HIVE_SQL = "../scripts/spark/connector/paimon-hive-test.sql";

    private PlParser parser;

    @Before
    public void initParser() {
        ExecutionEngineType.init(EngineType.SPARK, EngineTypeVersion.SPARK3);
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
        AresSparkStarter.main(starterArgs(HIVE_SQL));
    }

    @Ignore
    @Test
    public void filesystemCatalogOnHdfs() {
        AresSparkStarter.main(starterArgs(FILESYSTEM_SQL));
    }

    @Test
    public void paimonDoesNotNeedHadoopThirdPartyConnector() {
        assertFalse(Common.requiresHadoopThirdPartyConnector("paimon"));
        assertFalse(Common.requiresHadoopThirdPartyConnector(PaimonTables.CONNECTOR));
    }

    @Test
    public void filesystemScriptJoinsHdfsWarehouseAndRewritesDml() throws Exception {
        LogicalProject project = parser.parseToBaseBody(read(FILESYSTEM_SQL));
        assertTrue(PaimonTables.projectUsesFilesystem(project));
        assertTrue(PaimonTables.projectUsesHdfs(project));
        LogicalCreateSinkTable sink = sinkTable(project);
        assertEquals("hdfs://localhost:9000/paimon", sink.getOptions().get(PaimonTables.WAREHOUSE));
        assertEquals("paimon.default.t_user4", PaimonTables.qualifiedTable(sink.getOptions()));
        assertDml(project);
    }

    @Test
    public void hiveScriptUsesHiveCatalogAndRewritesDml() throws Exception {
        LogicalProject project = parser.parseToBaseBody(read(HIVE_SQL));
        assertFalse(PaimonTables.projectUsesFilesystem(project));
        assertFalse(PaimonTables.projectUsesHdfs(project));
        LogicalCreateSinkTable sink = sinkTable(project);
        assertEquals("default.t_user4", PaimonTables.qualifiedTable(sink.getOptions()));
        assertDml(project);
    }

    private static void assertDml(LogicalProject project) {
        LogicalUpdateSelectSQL simpleUpdate = null;
        LogicalUpdateSelectSQL joinUpdate = null;
        LogicalDeleteSelectSQL simpleDelete = null;
        LogicalDeleteSelectSQL joinDelete = null;
        LogicalMergeIntoSQL merge = null;
        for (LogicalOperation operation : flatten(project)) {
            if (operation instanceof LogicalUpdateSelectSQL) {
                LogicalUpdateSelectSQL update = (LogicalUpdateSelectSQL) operation;
                if (update.getPaimonSql().startsWith("UPDATE ")) {
                    simpleUpdate = update;
                } else {
                    joinUpdate = update;
                }
            } else if (operation instanceof LogicalDeleteSelectSQL) {
                LogicalDeleteSelectSQL delete = (LogicalDeleteSelectSQL) operation;
                if (delete.getPaimonSql().startsWith("DELETE ")) {
                    simpleDelete = delete;
                } else {
                    joinDelete = delete;
                }
            } else if (operation instanceof LogicalMergeIntoSQL) {
                merge = (LogicalMergeIntoSQL) operation;
            }
        }
        assertNotNull(simpleUpdate);
        assertTrue(simpleUpdate.getPaimonSql().contains(PaimonDml.TARGET));
        assertNotNull(simpleDelete);
        assertTrue(simpleDelete.getPaimonSql().startsWith("DELETE FROM "));
        assertNotNull(joinUpdate);
        assertTrue(joinUpdate.getPaimonSql().startsWith("MERGE INTO "));
        assertTrue(joinUpdate.getPaimonSql().contains("WHEN MATCHED THEN UPDATE SET"));
        assertNotNull(joinDelete);
        assertTrue(joinDelete.getPaimonSql().contains("WHEN MATCHED THEN DELETE"));
        assertNotNull(merge);
        assertTrue(merge.getPaimonSql().contains("WHEN MATCHED THEN UPDATE SET"));
        assertTrue(merge.getPaimonSql().contains("WHEN NOT MATCHED THEN INSERT"));
    }

    private static List<LogicalOperation> flatten(LogicalProject project) {
        List<LogicalOperation> operations = new ArrayList<>();
        for (LogicalOperation operation : project.getLogicalOperations()) {
            if (operation instanceof LogicalAnonymousBody) {
                operations.addAll(((LogicalAnonymousBody) operation).getAnonymousBody());
            } else {
                operations.add(operation);
            }
        }
        return operations;
    }

    private static LogicalCreateSinkTable sinkTable(LogicalProject project) {
        for (LogicalOperation operation : project.getLogicalOperations()) {
            if (operation instanceof LogicalCreateSinkTable) {
                return (LogicalCreateSinkTable) operation;
            }
        }
        throw new AssertionError("Paimon sink table was not parsed");
    }

    private static String read(String path) throws Exception {
        return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
    }

    private static String[] starterArgs(String sql) {
        return new String[] {
            "--master",
            Utils.getSparkMaster(),
            "--sql",
            sql,
            "--conf",
            "spark.jars=" + "../../ares-starter/ares-spark3-starter/target/ares-spark3-starter.jar"
        };
    }
}
