package com.github.ares.parser.test;

import com.github.ares.api.common.EngineType;
import com.github.ares.api.common.EngineTypeVersion;
import com.github.ares.api.common.ExecutionEngineType;
import com.github.ares.com.google.inject.Injector;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.common.utils.InjectorFactory;
import com.github.ares.parser.PlParser;
import com.github.ares.parser.config.ParserInjectorFactory;
import com.github.ares.parser.datasource.PropertiesDataSourcePatcher;
import com.github.ares.parser.datasource.SourceConfigPatcherFactory;
import com.github.ares.parser.hive.HiveTables;
import com.github.ares.parser.paimon.PaimonTables;
import com.github.ares.parser.plan.LogicalAnonymousBody;
import com.github.ares.parser.plan.LogicalCommit;
import com.github.ares.parser.plan.LogicalCreateHiveTable;
import com.github.ares.parser.plan.LogicalCreatePaimonTable;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalCreateTableAsSQL;
import com.github.ares.parser.plan.LogicalDeleteSelectSQL;
import com.github.ares.parser.plan.LogicalEndTransaction;
import com.github.ares.parser.plan.LogicalExceptionHandler;
import com.github.ares.parser.plan.LogicalExitLoop;
import com.github.ares.parser.plan.LogicalForLoop;
import com.github.ares.parser.plan.LogicalIfElse;
import com.github.ares.parser.plan.LogicalMergeIntoSQL;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalProject;
import com.github.ares.parser.plan.LogicalSelectSQL;
import com.github.ares.parser.plan.LogicalStartTransaction;
import com.github.ares.parser.plan.LogicalUpdateSelectSQL;
import com.github.ares.parser.plan.LogicalWhileLoop;
import com.github.ares.parser.utils.Constants;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PlParserTest {

    private PlParser plTransformation;

    @Before
    public void init() {
        ExecutionEngineType.init(EngineType.SPARK, EngineTypeVersion.SPARK3);
        Injector injector = ParserInjectorFactory.create();
        InjectorFactory.init(injector);
        plTransformation = injector.getInstance(PlParser.class);
        plTransformation.init();
        SourceConfigPatcherFactory.register(
                Constants.DEFAULT_DATASOURCE_PATCHER,
                new PropertiesDataSourcePatcher(new Properties()));
    }

    @Test
    public void parseCreateTable() {
        Injector injector = ParserInjectorFactory.create();
        InjectorFactory.init(injector);
        String pl =
                "CREATE TABLE test1\n"
                        + "USING jdbc\n"
                        + "OPTIONS (\n"
                        + "    'url'='jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false',\n"
                        + "    'driver'='com.mysql.cj.jdbc.Driver',\n"
                        + "    'user'='root',\n"
                        + "    'password'='123456',\n"
                        + "    'table_name'='t_user'\n"
                        + ");";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        long catalogTables =
                logicalProject.getLogicalOperations().stream()
                        .filter(
                                op ->
                                        op instanceof LogicalCreateSourceTable
                                                || op instanceof LogicalCreateSinkTable)
                        .count();
        Assert.assertEquals(2, catalogTables);
        LogicalCreateSourceTable logicalCreateSourceTable =
                (LogicalCreateSourceTable) logicalProject.getLogicalOperations().get(0);
        Assert.assertEquals("jdbc", logicalCreateSourceTable.getConnector());
        Assert.assertEquals("test1", logicalCreateSourceTable.getTableName());
        Assert.assertEquals(
                "jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false",
                logicalCreateSourceTable.getOptions().get("url"));
        Assert.assertEquals(
                "com.mysql.cj.jdbc.Driver", logicalCreateSourceTable.getOptions().get("driver"));
        Assert.assertEquals("root", logicalCreateSourceTable.getOptions().get("user"));
        Assert.assertEquals("123456", logicalCreateSourceTable.getOptions().get("password"));
        Assert.assertEquals("t_user", logicalCreateSourceTable.getOptions().get("table_name"));

        LogicalCreateSinkTable logicalCreateSinkTable =
                (LogicalCreateSinkTable) logicalProject.getLogicalOperations().get(1);
        Assert.assertEquals("jdbc", logicalCreateSinkTable.getConnector());
        Assert.assertEquals("test1", logicalCreateSinkTable.getTableName());
        Assert.assertEquals(
                "jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false",
                logicalCreateSinkTable.getOptions().get("url"));
        Assert.assertEquals(
                "com.mysql.cj.jdbc.Driver", logicalCreateSinkTable.getOptions().get("driver"));
        Assert.assertEquals("root", logicalCreateSinkTable.getOptions().get("user"));
        Assert.assertEquals("123456", logicalCreateSinkTable.getOptions().get("password"));
        Assert.assertEquals("t_user", logicalCreateSinkTable.getOptions().get("table_name"));
    }

    @Test
    public void parseCreateTableWithDs() {
        Injector injector = ParserInjectorFactory.create();
        InjectorFactory.init(injector);
        String pl =
                "SET datasource.mytest.connector=jdbc;\n"
                        + "SET datasource.mytest.url='jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false';\n"
                        + "SET datasource.mytest.driver=com.mysql.cj.jdbc.Driver;\n"
                        + "SET datasource.mytest.user=root;\n"
                        + "SET datasource.mytest.password=123456;\n"
                        + "\n"
                        + "CREATE TABLE test1\n"
                        + "USING jdbc\n"
                        + "OPTIONS (\n"
                        + "    'datasource' = 'mytest',\n"
                        + "    'table_name'='t_user'\n"
                        + ");";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        long catalogTables =
                logicalProject.getLogicalOperations().stream()
                        .filter(
                                op ->
                                        op instanceof LogicalCreateSourceTable
                                                || op instanceof LogicalCreateSinkTable)
                        .count();
        Assert.assertEquals(2, catalogTables);
        LogicalCreateSourceTable logicalCreateSourceTable =
                (LogicalCreateSourceTable) logicalProject.getLogicalOperations().get(0);
        Assert.assertEquals("jdbc", logicalCreateSourceTable.getConnector());
        Assert.assertEquals("test1", logicalCreateSourceTable.getTableName());
        Assert.assertEquals(
                "jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false",
                logicalCreateSourceTable.getOptions().get("url"));
        Assert.assertEquals(
                "com.mysql.cj.jdbc.Driver", logicalCreateSourceTable.getOptions().get("driver"));
        Assert.assertEquals("root", logicalCreateSourceTable.getOptions().get("user"));
        Assert.assertEquals("123456", logicalCreateSourceTable.getOptions().get("password"));
        Assert.assertEquals("t_user", logicalCreateSourceTable.getOptions().get("table_name"));

        LogicalCreateSinkTable logicalCreateSinkTable =
                (LogicalCreateSinkTable) logicalProject.getLogicalOperations().get(1);
        Assert.assertEquals("jdbc", logicalCreateSinkTable.getConnector());
        Assert.assertEquals("test1", logicalCreateSinkTable.getTableName());
        Assert.assertEquals(
                "jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false",
                logicalCreateSinkTable.getOptions().get("url"));
        Assert.assertEquals(
                "com.mysql.cj.jdbc.Driver", logicalCreateSinkTable.getOptions().get("driver"));
        Assert.assertEquals("root", logicalCreateSinkTable.getOptions().get("user"));
        Assert.assertEquals("123456", logicalCreateSinkTable.getOptions().get("password"));
        Assert.assertEquals("t_user", logicalCreateSinkTable.getOptions().get("table_name"));
    }

    @Test
    public void parseSetConfigWithQuotedValues() {
        Injector injector = ParserInjectorFactory.create();
        InjectorFactory.init(injector);
        String pl =
                "SET datasource.mytest.connector=mysql;\n"
                        + "SET datasource.mytest.driver='com.mysql.cj.jdbc.Driver';\n"
                        + "SET datasource.mytest.password='p@ss';\n"
                        + "\n"
                        + "CREATE TABLE test1\n"
                        + "USING mysql\n"
                        + "OPTIONS (\n"
                        + "    'datasource' = 'mytest',\n"
                        + "    'table_name'='t_user',\n"
                        + "    'type' = 'source'\n"
                        + ");";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        LogicalCreateSourceTable logicalCreateSourceTable =
                (LogicalCreateSourceTable) logicalProject.getLogicalOperations().get(0);
        Assert.assertEquals(
                "com.mysql.cj.jdbc.Driver", logicalCreateSourceTable.getOptions().get("driver"));
        Assert.assertEquals("p@ss", logicalCreateSourceTable.getOptions().get("password"));
    }

    @Test
    public void parseHiveTableAsView() {
        String pl =
                "CREATE TABLE test1\n"
                        + "USING hive\n"
                        + "OPTIONS (\n"
                        + "    'metastore_uri' = 'thrift://localhost:9083',\n"
                        + "    'table_name'='default.t_user',\n"
                        + "    'type' = 'source'\n"
                        + ");\n"
                        + "CREATE TABLE test2\n"
                        + "USING hive3\n"
                        + "OPTIONS (\n"
                        + "    'table_name'='default.t_user4',\n"
                        + "    'type' = 'sink,source'\n"
                        + ");";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        Assert.assertTrue(logicalProject.getSourceTables().isEmpty());

        LogicalCreateTableAsSQL sourceView = null;
        LogicalCreateTableAsSQL sinkView = null;
        LogicalCreateSinkTable sinkTable = null;
        for (LogicalOperation operation : logicalProject.getLogicalOperations()) {
            if (operation instanceof LogicalCreateTableAsSQL) {
                LogicalCreateTableAsSQL view = (LogicalCreateTableAsSQL) operation;
                if ("test1".equals(view.getTableName())) {
                    sourceView = view;
                } else if ("test2".equals(view.getTableName())) {
                    sinkView = view;
                }
            } else if (operation instanceof LogicalCreateSinkTable) {
                sinkTable = (LogicalCreateSinkTable) operation;
            }
        }
        Assert.assertNotNull(sourceView);
        Assert.assertTrue(sourceView.getSelectSQL().contains("default"));
        Assert.assertTrue(sourceView.getSelectSQL().contains("t_user"));
        Assert.assertNotNull(sinkView);
        Assert.assertTrue(sinkView.getSelectSQL().contains("t_user4"));
        Assert.assertNotNull(sinkTable);
        Assert.assertEquals("hive", sinkTable.getConnector());
        Assert.assertEquals("test2", sinkTable.getTableName());
        Assert.assertEquals("default.t_user4", sinkTable.getOptions().get("table_name"));
    }

    @Test
    public void parseTransactionSegment() {
        String pl =
                "START TRANSACTION;\n"
                        + "SELECT CASE WHEN id > 0 THEN name ELSE '' END FROM test2;\n"
                        + "COMMIT;\n"
                        + "END TRANSACTION;\n";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        LogicalAnonymousBody body =
                (LogicalAnonymousBody) logicalProject.getLogicalOperations().get(0);
        Assert.assertEquals(4, body.getAnonymousBody().size());
        Assert.assertTrue(body.getAnonymousBody().get(0) instanceof LogicalStartTransaction);
        LogicalSelectSQL select = (LogicalSelectSQL) body.getAnonymousBody().get(1);
        Assert.assertTrue(select.getOriginSQL().toUpperCase().contains("END"));
        Assert.assertTrue(body.getAnonymousBody().get(2) instanceof LogicalCommit);
        Assert.assertTrue(body.getAnonymousBody().get(3) instanceof LogicalEndTransaction);
    }

    @Test
    public void parseEndTerminatedControlFlow() {
        String pl =
                "def a = 1;\n"
                        + "if a > 0\n"
                        + "    PUT_LINE(a);\n"
                        + "elsif a = 0\n"
                        + "    a = a + 1;\n"
                        + "elseif a < 0\n"
                        + "    a = a - 1;\n"
                        + "else\n"
                        + "    PUT_LINE(a);\n"
                        + "end;\n"
                        + "while a < 10\n"
                        + "    if a = 3\n"
                        + "        break;\n"
                        + "    end;\n"
                        + "    END TRANSACTION;\n"
                        + "end\n"
                        + "for i in 1 .. a\n"
                        + "    PUT_LINE(i);\n"
                        + "end;\n"
                        + "try\n"
                        + "    START TRANSACTION;\n"
                        + "    SELECT CASE WHEN id > 0 THEN name ELSE '' END FROM test2;\n"
                        + "    END TRANSACTION;\n"
                        + "catch\n"
                        + "    ROLLBACK;\n"
                        + "    END TRANSACTION;\n"
                        + "    raise;\n"
                        + "end;\n";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        LogicalAnonymousBody body =
                (LogicalAnonymousBody) logicalProject.getLogicalOperations().get(0);
        List<LogicalOperation> ops = body.getAnonymousBody();
        Assert.assertEquals(5, ops.size());

        LogicalIfElse ifElse = (LogicalIfElse) ops.get(1);
        Assert.assertEquals(1, ifElse.getIfBody().size());
        Assert.assertEquals(2, ifElse.getElseIfs().size());
        Assert.assertEquals(1, ifElse.getElseIfs().get(0).getIfBody().size());
        Assert.assertEquals(1, ifElse.getElseIfs().get(1).getIfBody().size());
        Assert.assertEquals(1, ifElse.getElseBody().size());

        LogicalWhileLoop whileLoop = (LogicalWhileLoop) ops.get(2);
        Assert.assertEquals(2, whileLoop.getWhileBody().size());
        LogicalIfElse nested = (LogicalIfElse) whileLoop.getWhileBody().get(0);
        Assert.assertTrue(nested.getIfBody().get(0) instanceof LogicalExitLoop);
        Assert.assertTrue(whileLoop.getWhileBody().get(1) instanceof LogicalEndTransaction);

        LogicalForLoop forLoop = (LogicalForLoop) ops.get(3);
        Assert.assertEquals("i", forLoop.getIndexName());
        Assert.assertEquals(1, forLoop.getForBody().size());

        LogicalExceptionHandler handler = (LogicalExceptionHandler) ops.get(4);
        Assert.assertEquals(3, handler.getTryBody().size());
        Assert.assertTrue(handler.getTryBody().get(0) instanceof LogicalStartTransaction);
        Assert.assertTrue(handler.getTryBody().get(2) instanceof LogicalEndTransaction);
        Assert.assertEquals(2, handler.getExHandlerBody().size());
        Assert.assertTrue(handler.getExHandlerBody().get(1) instanceof LogicalEndTransaction);
        Assert.assertEquals(Boolean.TRUE, handler.getWithRaise());
    }

    @Test(expected = ParseException.class)
    public void parseHiveTableRequiresTableName() {
        String pl =
                "CREATE TABLE test1\n"
                        + "USING hive\n"
                        + "OPTIONS (\n"
                        + "    'type' = 'source'\n"
                        + ");";
        plTransformation.parseToBaseBody(pl);
    }

    @Test
    public void parsePaimonHiveCatalogAsView() {
        String pl =
                "CREATE TABLE test1\n"
                        + "USING paimon\n"
                        + "OPTIONS (\n"
                        + "    'metastore' = 'hive',\n"
                        + "    'table_name'='default.t_user',\n"
                        + "    'type' = 'source'\n"
                        + ");\n"
                        + "CREATE TABLE test2\n"
                        + "USING paimon\n"
                        + "OPTIONS (\n"
                        + "    'metastore' = 'hive',\n"
                        + "    'table_name'='default.t_user4',\n"
                        + "    'type' = 'sink,source'\n"
                        + ");";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        Assert.assertTrue(logicalProject.getSourceTables().isEmpty());

        LogicalCreateTableAsSQL sourceView = null;
        LogicalCreateSinkTable sinkTable = null;
        for (LogicalOperation operation : logicalProject.getLogicalOperations()) {
            if (operation instanceof LogicalCreateTableAsSQL
                    && "test1".equals(((LogicalCreateTableAsSQL) operation).getTableName())) {
                sourceView = (LogicalCreateTableAsSQL) operation;
            } else if (operation instanceof LogicalCreateSinkTable) {
                sinkTable = (LogicalCreateSinkTable) operation;
            }
        }
        Assert.assertNotNull(sourceView);
        Assert.assertTrue(sourceView.getSelectSQL().contains("default"));
        Assert.assertTrue(sourceView.getSelectSQL().contains("t_user"));
        Assert.assertFalse(sourceView.getSelectSQL().contains("paimon"));
        Assert.assertEquals("hive", sourceView.getProperties().get("metastore"));
        Assert.assertNotNull(sinkTable);
        Assert.assertEquals("paimon", sinkTable.getConnector());
        Assert.assertEquals("default.t_user4", sinkTable.getOptions().get("table_name"));
        Assert.assertEquals("hive", sinkTable.getOptions().get("metastore"));
    }

    @Test
    public void parsePaimonFilesystemJoinsHdfsWarehouse() {
        String pl =
                "CREATE TABLE test1\n"
                        + "USING paimon\n"
                        + "OPTIONS (\n"
                        + "    'metastore' = 'filesystem',\n"
                        + "    'fs.defaultFS' = 'hdfs://localhost:9000',\n"
                        + "    'warehouse' = '/paimon',\n"
                        + "    'hdfs_site_path' = '/etc/hadoop/hdfs-site.xml',\n"
                        + "    'table_name'='default.t_user',\n"
                        + "    'type' = 'sink,source'\n"
                        + ");";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        LogicalCreateTableAsSQL sourceView = null;
        LogicalCreateSinkTable sinkTable = null;
        for (LogicalOperation operation : logicalProject.getLogicalOperations()) {
            if (operation instanceof LogicalCreateTableAsSQL) {
                sourceView = (LogicalCreateTableAsSQL) operation;
            } else if (operation instanceof LogicalCreateSinkTable) {
                sinkTable = (LogicalCreateSinkTable) operation;
            }
        }
        Assert.assertNotNull(sourceView);
        Assert.assertTrue(sourceView.getSelectSQL().contains("paimon"));
        Assert.assertTrue(sourceView.getSelectSQL().contains("default"));
        Assert.assertTrue(sourceView.getSelectSQL().contains("t_user"));
        Assert.assertEquals(
                "hdfs://localhost:9000/paimon", sourceView.getProperties().get("warehouse"));
        Assert.assertEquals("/etc/hadoop", PaimonTables.hadoopConfDir(sourceView.getProperties()));
        Assert.assertNotNull(sinkTable);
        Assert.assertTrue(PaimonTables.usesHdfs(sinkTable.getOptions()));
        Assert.assertTrue(PaimonTables.projectUsesFilesystem(logicalProject));
        Assert.assertTrue(PaimonTables.projectUsesHdfs(logicalProject));
        Assert.assertEquals(
                "`paimon`.`default`.`t_user`",
                PaimonTables.quoteTable(PaimonTables.qualifiedTable(sinkTable.getOptions())));
    }

    @Test(expected = ParseException.class)
    public void parsePaimonRequiresMetastore() {
        String pl =
                "CREATE TABLE test1\n"
                        + "USING paimon\n"
                        + "OPTIONS (\n"
                        + "    'table_name'='default.t_user',\n"
                        + "    'type' = 'source'\n"
                        + ");";
        plTransformation.parseToBaseBody(pl);
    }

    @Test(expected = ParseException.class)
    public void parsePaimonFilesystemRequiresWarehouse() {
        String pl =
                "CREATE TABLE test1\n"
                        + "USING paimon\n"
                        + "OPTIONS (\n"
                        + "    'metastore' = 'filesystem',\n"
                        + "    'table_name'='default.t_user',\n"
                        + "    'type' = 'source'\n"
                        + ");";
        plTransformation.parseToBaseBody(pl);
    }

    @Test
    public void paimonDmlRewritesToSparkSql() {
        String pl =
                "CREATE TABLE test2\n"
                        + "USING paimon\n"
                        + "OPTIONS (\n"
                        + "    'metastore' = 'hive',\n"
                        + "    'table_name'='default.t_user',\n"
                        + "    'type' = 'sink,source'\n"
                        + ");\n"
                        + "UPDATE test2 SET name = 'a' WHERE id = 1;\n"
                        + "DELETE FROM test2 WHERE id = 2;\n"
                        + "UPDATE test2 a, test1 b SET a.name = b.name WHERE a.id = b.id;\n"
                        + "DELETE FROM test2 a, test1 b WHERE a.id = b.id;\n"
                        + "MERGE INTO test2 t\n"
                        + "USING test1 s\n"
                        + "ON t.id = s.id\n"
                        + "WHEN MATCHED THEN UPDATE SET t.name = s.name\n"
                        + "WHEN NOT MATCHED THEN INSERT (t.id, t.name) VALUES (s.id, s.name);\n";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        List<LogicalOperation> operations = new ArrayList<>();
        for (LogicalOperation operation : logicalProject.getLogicalOperations()) {
            if (operation instanceof LogicalAnonymousBody) {
                operations.addAll(((LogicalAnonymousBody) operation).getAnonymousBody());
            } else {
                operations.add(operation);
            }
        }
        LogicalUpdateSelectSQL simpleUpdate = null;
        LogicalUpdateSelectSQL joinUpdate = null;
        LogicalDeleteSelectSQL simpleDelete = null;
        LogicalDeleteSelectSQL joinDelete = null;
        LogicalMergeIntoSQL merge = null;
        for (LogicalOperation operation : operations) {
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
        Assert.assertNotNull(simpleUpdate);
        Assert.assertTrue(simpleUpdate.getPaimonSql().contains("__ARES_PAIMON_TARGET__"));
        Assert.assertTrue(simpleUpdate.getPaimonSql().contains("WHERE"));
        Assert.assertNotNull(simpleDelete);
        Assert.assertTrue(
                simpleDelete.getPaimonSql().startsWith("DELETE FROM __ARES_PAIMON_TARGET__"));
        Assert.assertNotNull(joinUpdate);
        Assert.assertTrue(
                joinUpdate.getPaimonSql().startsWith("MERGE INTO __ARES_PAIMON_TARGET__"));
        Assert.assertTrue(joinUpdate.getPaimonSql().contains("WHEN MATCHED THEN UPDATE SET"));
        Assert.assertTrue(joinUpdate.getPaimonSql().contains("test1"));
        Assert.assertNotNull(joinDelete);
        Assert.assertTrue(joinDelete.getPaimonSql().contains("WHEN MATCHED THEN DELETE"));
        Assert.assertNotNull(merge);
        Assert.assertTrue(merge.getPaimonSql().contains("WHEN MATCHED THEN UPDATE SET"));
        Assert.assertTrue(merge.getPaimonSql().contains("WHEN NOT MATCHED THEN INSERT"));
        Assert.assertTrue(merge.getPaimonSql().contains("`name`"));
    }

    @Test
    public void parseHiveNativeCreateTable() {
        String pl =
                "CREATE TABLE IF NOT EXISTS default.t_user (\n"
                        + "    id INT COMMENT 'id',\n"
                        + "    name STRING,\n"
                        + "    price DECIMAL(10, 2),\n"
                        + "    tags ARRAY<STRING>,\n"
                        + "    address STRUCT<street:STRING, city:STRING>,\n"
                        + "    c_time TIMESTAMP\n"
                        + ")\n"
                        + "COMMENT 'user table'\n"
                        + "PARTITIONED BY (dt STRING)\n"
                        + "CLUSTERED BY (id) SORTED BY (id) INTO 4 BUCKETS\n"
                        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','\n"
                        + "STORED AS PARQUET\n"
                        + "LOCATION '/warehouse/t_user'\n"
                        + "TBLPROPERTIES ('parquet.compression' = 'SNAPPY');\n"
                        + "CREATE EXTERNAL TABLE ext_user (id BIGINT) STORED AS ORC LOCATION '/data/ext';\n"
                        + "CREATE TABLE default.t_copy LIKE default.t_user;\n"
                        + "INSERT INTO default.t_user SELECT id, name FROM src;\n";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        List<LogicalCreateHiveTable> created = new ArrayList<>();
        for (LogicalOperation operation : logicalProject.getLogicalOperations()) {
            if (operation instanceof LogicalCreateHiveTable) {
                created.add((LogicalCreateHiveTable) operation);
            }
        }
        Assert.assertEquals(3, created.size());
        Assert.assertTrue(created.get(0).getSql().toUpperCase().contains("STORED AS PARQUET"));
        Assert.assertTrue(created.get(0).getSql().toUpperCase().contains("PARTITIONED BY"));
        Assert.assertEquals("default.t_user", created.get(0).getTableName());
        Assert.assertTrue(created.get(1).getSql().toUpperCase().contains("EXTERNAL"));
        Assert.assertEquals("ext_user", created.get(1).getTableName());
        Assert.assertTrue(created.get(2).getSql().toUpperCase().contains("LIKE"));
        Assert.assertEquals("default.t_copy", created.get(2).getTableName());

        LogicalCreateSinkTable sink = null;
        for (LogicalCreateSinkTable table : logicalProject.getSinkTables()) {
            if ("default.t_user".equals(table.getTableName())) {
                sink = table;
            }
        }
        Assert.assertNotNull(sink);
        Assert.assertEquals("hive", sink.getConnector());
        Assert.assertEquals("default.t_user", sink.getOptions().get("table_name"));
        Assert.assertTrue(HiveTables.scriptUsesHive(pl));
    }

    @Test
    public void parseCreateTableAsSelectStaysTempView() {
        String pl =
                "CREATE TABLE t AS SELECT 1 AS id;\n"
                        + "CREATE TABLE u (id INT) AS SELECT 1 AS id;";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        for (LogicalOperation operation : logicalProject.getLogicalOperations()) {
            Assert.assertFalse(operation instanceof LogicalCreateHiveTable);
        }
        LogicalAnonymousBody body =
                (LogicalAnonymousBody) logicalProject.getLogicalOperations().get(0);
        Assert.assertEquals(2, body.getAnonymousBody().size());
        Assert.assertTrue(body.getAnonymousBody().get(0) instanceof LogicalCreateTableAsSQL);
        Assert.assertTrue(body.getAnonymousBody().get(1) instanceof LogicalCreateTableAsSQL);
        Assert.assertFalse(HiveTables.scriptUsesHive(pl));
    }

    @Test
    public void parseHiveCreateTableAsSelect() {
        String pl = "CREATE TABLE default.t_user STORED AS PARQUET AS SELECT id, name FROM src;";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        LogicalCreateHiveTable created = null;
        for (LogicalOperation operation : logicalProject.getLogicalOperations()) {
            if (operation instanceof LogicalCreateHiveTable) {
                created = (LogicalCreateHiveTable) operation;
            }
        }
        Assert.assertNotNull(created);
        Assert.assertTrue(created.getSql().toUpperCase().contains("AS SELECT"));
        Assert.assertTrue(HiveTables.scriptUsesHive(pl));
        Assert.assertFalse(
                HiveTables.scriptUsesHive(
                        "CREATE TABLE t (id INT) USING jdbc OPTIONS ('table_name'='t');"));
    }

    @Test
    public void parsePaimonCreateTable() {
        String filesystem =
                "CREATE TABLE t_user (\n"
                        + "    id BIGINT COMMENT 'id',\n"
                        + "    name STRING,\n"
                        + "    dt STRING,\n"
                        + "    PRIMARY KEY (id, dt) NOT ENFORCED\n"
                        + ")\n"
                        + "USING paimon\n"
                        + "COMMENT 'users'\n"
                        + "PARTITIONED BY (dt)\n"
                        + "OPTIONS (\n"
                        + "    'metastore' = 'filesystem',\n"
                        + "    'warehouse' = '/paimon',\n"
                        + "    'table_name' = 'default.t_user',\n"
                        + "    'bucket' = '4',\n"
                        + "    'type' = 'source,sink'\n"
                        + ");";
        LogicalProject project = plTransformation.parseToBaseBody(filesystem);
        LogicalCreatePaimonTable created = null;
        LogicalCreateTableAsSQL sourceView = null;
        LogicalCreateSinkTable sink = null;
        for (LogicalOperation operation : project.getLogicalOperations()) {
            if (operation instanceof LogicalCreatePaimonTable) {
                created = (LogicalCreatePaimonTable) operation;
            } else if (operation instanceof LogicalCreateTableAsSQL) {
                sourceView = (LogicalCreateTableAsSQL) operation;
            } else if (operation instanceof LogicalCreateSinkTable) {
                sink = (LogicalCreateSinkTable) operation;
            }
        }
        Assert.assertNotNull(created);
        String ddl = created.getSql().toLowerCase();
        Assert.assertTrue(ddl.contains("`paimon`.`default`.`t_user`"));
        Assert.assertTrue(ddl.contains("using paimon"));
        Assert.assertTrue(ddl.contains("'primary-key'='id,dt'"));
        Assert.assertTrue(ddl.contains("'bucket'='4'"));
        Assert.assertTrue(ddl.contains("partitioned by (`dt`)"));
        Assert.assertTrue(ddl.contains("comment 'users'"));
        Assert.assertFalse(ddl.contains("warehouse"));
        Assert.assertFalse(ddl.contains("primary key"));
        Assert.assertNotNull(sourceView);
        Assert.assertEquals("t_user", sourceView.getTableName());
        Assert.assertTrue(sourceView.getSelectSQL().contains("paimon"));
        Assert.assertNotNull(sink);
        Assert.assertEquals("paimon", sink.getConnector());
        Assert.assertEquals("filesystem", sink.getOptions().get("metastore"));
        Assert.assertEquals("/paimon", sink.getOptions().get("warehouse"));
        Assert.assertNull(sink.getOptions().get("bucket"));
        Assert.assertTrue(PaimonTables.projectUsesFilesystem(project));

        String hive =
                "CREATE TABLE IF NOT EXISTS t_hive (\n"
                        + "    id BIGINT,\n"
                        + "    PRIMARY KEY (id) NOT ENFORCED\n"
                        + ")\n"
                        + "USING paimon\n"
                        + "OPTIONS (\n"
                        + "    'metastore' = 'hive',\n"
                        + "    'table_name' = 'default.t_hive'\n"
                        + ");";
        LogicalProject hiveProject = plTransformation.parseToBaseBody(hive);
        LogicalCreatePaimonTable hiveCreated = null;
        for (LogicalOperation operation : hiveProject.getLogicalOperations()) {
            if (operation instanceof LogicalCreatePaimonTable) {
                hiveCreated = (LogicalCreatePaimonTable) operation;
            }
        }
        Assert.assertNotNull(hiveCreated);
        Assert.assertTrue(hiveCreated.getSql().toUpperCase().contains("IF NOT EXISTS"));
        Assert.assertTrue(hiveCreated.getSql().contains("`default`.`t_hive`"));
        Assert.assertFalse(hiveCreated.getSql().contains("`paimon`"));
        Assert.assertTrue(hiveCreated.getSql().contains("'primary-key'='id'"));
    }
}
