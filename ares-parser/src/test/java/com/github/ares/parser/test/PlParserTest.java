package com.github.ares.parser.test;

import com.github.ares.com.google.inject.Guice;
import com.github.ares.com.google.inject.Injector;
import com.github.ares.common.utils.InjectorFactory;
import com.github.ares.parser.PlParser;
import com.github.ares.parser.config.ParserServiceModule;
import com.github.ares.parser.datasource.SourceConfigPatcherFactory;
import com.github.ares.parser.plan.LogicalProject;
import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.SQLParserFactory;
import com.github.ares.parser.sqlparser.SQLParserFactoryLoader;
import com.github.ares.parser.sqlparser.model.SQLDelete;
import com.github.ares.parser.sqlparser.model.SQLInsert;
import com.github.ares.parser.sqlparser.model.SQLMerge;
import com.github.ares.parser.sqlparser.model.SQLSelect;
import com.github.ares.parser.sqlparser.model.SQLUpdate;
import com.github.ares.parser.utils.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class PlParserTest {

    @Test
    public void test01() throws IOException {
        Injector injector = Guice.createInjector(new ParserServiceModule());
        InjectorFactory.init(injector);
        PlParser plTransformation = injector.getInstance(PlParser.class);
        plTransformation.init();
        InputStream in = this.getClass().getClassLoader().getResourceAsStream("mysql.sql");
        LogicalProject baseBody = plTransformation.parseToBaseBody(in);
        Assert.assertFalse(baseBody.getLogicalOperations().isEmpty());
        in.close();
    }

    @Test
    public void test02() throws Exception {
//        String sql = "delete from table1 a where a.id<>abs(-1) and (name='a' or name like 'b') and a.age in (12,14,17)";
        String sql = "delete from table1 a, table2 b where a.id=b.id";
        SQLParserFactory sqlParserFactory = SQLParserFactoryLoader.getDefaultFactory();
        SQLParser sqlParser = sqlParserFactory.getParser();
        SQLDelete sqlDelete = sqlParser.parseDelete(sql);
        sqlDelete = sqlDelete;
    }

    @Test
    public void test03() throws Exception {
//        String sql = "update table1 a set a.name='abc',a.age=12 where a.id<>abs(-1) and (name='a' or name like 'b') and a.age in (12,14,17)";
        String sql = "update table1 a, (select CASE \n" +
                "        WHEN salary > 100000 THEN 'High Salary'\n" +
                "        WHEN salary BETWEEN 50000 AND 100000 THEN 'Medium Salary'\n" +
                "        ELSE 'Low Salary'\n" +
                "    END AS salary_level from table2) b set a.name=b.name,a.age=b.age where a.id=b.id and a.name not like 'xx'";
        SQLParserFactory sqlParserFactory = SQLParserFactoryLoader.getDefaultFactory();
        SQLParser sqlParser = sqlParserFactory.getParser();
        SQLUpdate sqlUpdate = sqlParser.parseUpdate(sql);
        sqlUpdate = sqlUpdate;
    }

    @Test
    public void test04() throws Exception {
        String sql = "insert into table1 (id, name, age) values (1, 'abc', 12), (2, 'def', 14), (3, 'ghi', 17)";
//        String sql = "insert into table1 (id, name, age) select * from table2";
        SQLParserFactory sqlParserFactory = SQLParserFactoryLoader.getDefaultFactory();
        SQLParser sqlParser = sqlParserFactory.getParser();
        SQLInsert sqlInsert = sqlParser.parseInsert(sql);
        sqlInsert = sqlInsert;
    }

    @Test
    public void test05() throws Exception {
        String sql = "select /*+ mapjoin(a) */ *  from table1 where id=1";
        sql = "select /*+ mapjoin ( asd,1 ) */ * from test11 where id > 0 ";
//        String sql = "insert into table1 (id, name, age) select * from table2";
        SQLParserFactory sqlParserFactory = SQLParserFactoryLoader.getDefaultFactory();
        SQLParser sqlParser = sqlParserFactory.getParser();
        SQLSelect sqlSelect = sqlParser.parseSelect(sql);
        sqlSelect = sqlSelect;
    }

    @Test
    public void test06() throws Exception {
        String sql = "merge into table1 a using table2 b on (a.id=b.id and a.name=b.name) " +
                "when matched then update set a.name = b.name, a.age = b.age where a.id<>-1 " +
                "when not matched then insert (id, name, age) values (b.id, b.name, b.age)";
//        String sql = "insert into table1 (id, name, age) select * from table2";
        SQLParserFactory sqlParserFactory = SQLParserFactoryLoader.getDefaultFactory();
        SQLParser sqlParser = sqlParserFactory.getParser();
        SQLMerge sqlMerge = sqlParser.parseMerge(sql);
        sqlMerge = sqlMerge;
    }
}
