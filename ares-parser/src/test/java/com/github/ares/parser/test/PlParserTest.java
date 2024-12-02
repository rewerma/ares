package com.github.ares.parser.test;

import com.github.ares.api.common.EngineType;
import com.github.ares.api.common.EngineTypeVersion;
import com.github.ares.api.common.ExecutionEngineType;
import com.github.ares.com.google.inject.Guice;
import com.github.ares.com.google.inject.Injector;
import com.github.ares.common.utils.InjectorFactory;
import com.github.ares.parser.PlParser;
import com.github.ares.parser.config.ParserServiceModule;
import com.github.ares.parser.datasource.PropertiesDataSourceComplement;
import com.github.ares.parser.datasource.SourceConfigComplementFactory;
import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalProject;
import com.github.ares.parser.utils.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

@Ignore
public class PlParserTest {

    private PlParser plTransformation;

    @Before
    public void init() {
        ExecutionEngineType.init(EngineType.SPARK, EngineTypeVersion.SPARK3);
        Injector injector = Guice.createInjector(new ParserServiceModule());
        InjectorFactory.init(injector);
        plTransformation = injector.getInstance(PlParser.class);
        plTransformation.init();
        SourceConfigComplementFactory.register(Constants.DEFAULT_DATASOURCE_COMPLEMENT,
                new PropertiesDataSourceComplement(new Properties()));
    }

    @Test
    public void parseCreateTable() {
        String pl = "CREATE TABLE test1\n" +
                "WITH (\n" +
                "    'connector'='jdbc',\n" +
                "    'url'='jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false',\n" +
                "    'driver'='com.mysql.cj.jdbc.Driver',\n" +
                "    'user'='root',\n" +
                "    'password'='123456',\n" +
                "    -- 'query'='select * from t_user',\n" +
                "    'table_name'='t_user',\n" +
                "    'type' = 'source,sink'\n" +
                ");";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        Assert.assertEquals(2, logicalProject.getLogicalOperations().size());
        LogicalCreateSourceTable logicalCreateSourceTable = (LogicalCreateSourceTable) logicalProject.getLogicalOperations().get(0);
        Assert.assertEquals("jdbc", logicalCreateSourceTable.getConnector());
        Assert.assertEquals("test1", logicalCreateSourceTable.getTableName());
        Assert.assertEquals("jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false", logicalCreateSourceTable.getOptions().get("url"));
        Assert.assertEquals("com.mysql.cj.jdbc.Driver", logicalCreateSourceTable.getOptions().get("driver"));
        Assert.assertEquals("root", logicalCreateSourceTable.getOptions().get("user"));
        Assert.assertEquals("123456", logicalCreateSourceTable.getOptions().get("password"));
        Assert.assertEquals("t_user", logicalCreateSourceTable.getOptions().get("table_name"));

        LogicalCreateSinkTable logicalCreateSinkTable = (LogicalCreateSinkTable) logicalProject.getLogicalOperations().get(1);
        Assert.assertEquals("jdbc", logicalCreateSinkTable.getConnector());
        Assert.assertEquals("test1", logicalCreateSinkTable.getTableName());
        Assert.assertEquals("jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false", logicalCreateSinkTable.getOptions().get("url"));
        Assert.assertEquals("com.mysql.cj.jdbc.Driver", logicalCreateSinkTable.getOptions().get("driver"));
        Assert.assertEquals("root", logicalCreateSinkTable.getOptions().get("user"));
        Assert.assertEquals("123456", logicalCreateSinkTable.getOptions().get("password"));
        Assert.assertEquals("t_user", logicalCreateSinkTable.getOptions().get("table_name"));
    }

    @Test
    public void parseCreateTableWithDs() {
        String pl = "SET datasource.mytest.connector=jdbc;\n" +
                "SET datasource.mytest.url=jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false;\n" +
                "SET datasource.mytest.driver=com.mysql.cj.jdbc.Driver;\n" +
                "SET datasource.mytest.user=root;\n" +
                "SET datasource.mytest.password=123456;\n" +
                "\n" +
                "CREATE TABLE test1\n" +
                "WITH (\n" +
                "    'datasource' = 'mytest',\n" +
                "    -- 'query'='select * from t_user',\n" +
                "    'table_name'='t_user',\n" +
                "    'type' = 'source,sink'\n" +
                ");";
        LogicalProject logicalProject = plTransformation.parseToBaseBody(pl);
        Assert.assertEquals(2, logicalProject.getLogicalOperations().size());
        LogicalCreateSourceTable logicalCreateSourceTable = (LogicalCreateSourceTable) logicalProject.getLogicalOperations().get(0);
        Assert.assertEquals("jdbc", logicalCreateSourceTable.getConnector());
        Assert.assertEquals("test1", logicalCreateSourceTable.getTableName());
        Assert.assertEquals("jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false", logicalCreateSourceTable.getOptions().get("url"));
        Assert.assertEquals("com.mysql.cj.jdbc.Driver", logicalCreateSourceTable.getOptions().get("driver"));
        Assert.assertEquals("root", logicalCreateSourceTable.getOptions().get("user"));
        Assert.assertEquals("123456", logicalCreateSourceTable.getOptions().get("password"));
        Assert.assertEquals("t_user", logicalCreateSourceTable.getOptions().get("table_name"));

        LogicalCreateSinkTable logicalCreateSinkTable = (LogicalCreateSinkTable) logicalProject.getLogicalOperations().get(1);
        Assert.assertEquals("jdbc", logicalCreateSinkTable.getConnector());
        Assert.assertEquals("test1", logicalCreateSinkTable.getTableName());
        Assert.assertEquals("jdbc:mysql://127.0.0.1:3306/mytest?useSSL=false", logicalCreateSinkTable.getOptions().get("url"));
        Assert.assertEquals("com.mysql.cj.jdbc.Driver", logicalCreateSinkTable.getOptions().get("driver"));
        Assert.assertEquals("root", logicalCreateSinkTable.getOptions().get("user"));
        Assert.assertEquals("123456", logicalCreateSinkTable.getOptions().get("password"));
        Assert.assertEquals("t_user", logicalCreateSinkTable.getOptions().get("table_name"));
    }

    @Test
    public void parseDataSources() {
        String pl =
                "CREATE TABLE test1\n" +
                "WITH (\n" +
                "    'datasource' = 'mytest',\n" +
                "    -- 'query'='select * from t_user',\n" +
                "    'table_name'='t_user',\n" +
                "    'type' = 'source,sink'\n" +
                ");";
        List<String> dataSources = plTransformation.parseDataSources(pl);
        Assert.assertEquals(1, dataSources.size());
        Assert.assertEquals("mytest", dataSources.get(0));
    }
}
