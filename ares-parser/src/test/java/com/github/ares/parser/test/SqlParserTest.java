package com.github.ares.parser.test;

import com.github.ares.api.common.EngineType;
import com.github.ares.api.common.EngineTypeVersion;
import com.github.ares.api.common.ExecutionEngineType;
import com.github.ares.parser.sqlparser.SQLParser;
import com.github.ares.parser.sqlparser.SQLParserFactory;
import com.github.ares.parser.sqlparser.SQLParserFactoryLoader;
import com.github.ares.parser.sqlparser.model.SQLDelete;
import com.github.ares.parser.sqlparser.model.SQLInsert;
import com.github.ares.parser.sqlparser.model.SQLMerge;
import com.github.ares.parser.sqlparser.model.SQLSelect;
import com.github.ares.parser.sqlparser.model.SQLUpdate;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SqlParserTest {
    private SQLParser sqlParser;

    @Before
    public void init() {
        ExecutionEngineType.init(EngineType.SPARK, EngineTypeVersion.SPARK3);
        SQLParserFactory sqlParserFactory = SQLParserFactoryLoader.getDefaultFactory();
        sqlParser = sqlParserFactory.getParser();
    }

    @Test
    public void testInsert() {
        String sql = "insert into table1 (id, name, age) values (1, 'abc', 12), (2, 'def', 14), (3, 'ghi', 17)";
        SQLInsert sqlInsert = sqlParser.parseInsert(sql);
        Assert.assertNotNull(sqlInsert.getTable());
        Assert.assertFalse(sqlInsert.getColumns().isEmpty());
        Assert.assertEquals(3, sqlInsert.getValuesArray().size());
        Assert.assertEquals("1", sqlInsert.getValuesArray().get(0).get(0));
        Assert.assertEquals("'abc'", sqlInsert.getValuesArray().get(0).get(1));
        Assert.assertNotNull(sqlInsert.getSourceSql());

        sql = "insert into table1 values (1, 'abc', 12)";
        sqlInsert = sqlParser.parseInsert(sql);
        Assert.assertNotNull(sqlInsert.getTable());
        Assert.assertTrue(sqlInsert.getColumns().isEmpty());
        Assert.assertEquals(1, sqlInsert.getValuesArray().size());
        Assert.assertEquals("1", sqlInsert.getValuesArray().get(0).get(0));
        Assert.assertEquals("'abc'", sqlInsert.getValuesArray().get(0).get(1));
        Assert.assertNotNull(sqlInsert.getSourceSql());

        sql = "insert into table1 select a.id, a.name, b.role_name from table1 a left join role b on a.role_id=b.id where a.id>0";
        sqlInsert = sqlParser.parseInsert(sql);
        Assert.assertNotNull(sqlInsert.getTable());
        Assert.assertTrue(sqlInsert.getColumns().isEmpty());
        Assert.assertNull(sqlInsert.getValuesArray());
        Assert.assertNotNull(sqlInsert.getSourceSql());
        System.out.println(sqlInsert.getSourceSql());

        sql = "insert into table1 (id, name, role_name) select /*+ mapjoin(b) */ /*+ cache()*/ a.id, a.name, b.role_name " +
                "from table1 a left join role b on a.role_id=b.id where a.id>0";
        sqlInsert = sqlParser.parseInsert(sql);
        Assert.assertNotNull(sqlInsert.getTable());
        Assert.assertFalse(sqlInsert.getColumns().isEmpty());
        Assert.assertNull(sqlInsert.getValuesArray());
        Assert.assertNotNull(sqlInsert.getSourceSql());
        Assert.assertNotNull(sqlInsert.getHints());
        Assert.assertEquals(2, sqlInsert.getHints().size());
        Assert.assertEquals("mapjoin", sqlInsert.getHints().get(0).getHintName());
        Assert.assertFalse(sqlInsert.getHints().get(0).getArguments().isEmpty());
        Assert.assertEquals("b", sqlInsert.getHints().get(0).getArguments().get(0));
        System.out.println(sqlInsert.getSourceSql());
    }

    @Test
    public void testUpdate() {
        String sql = "update table1 a set a.name='abc',a.age=12 where a.id<>abs(-1) and (name='a' or name like 'b') and a.age in (12,14,17)";
        SQLUpdate sqlUpdate = sqlParser.parseUpdate(sql);
        Assert.assertNotNull(sqlUpdate.getTable());
        Assert.assertFalse(sqlUpdate.getUpdateColumns().isEmpty());
        Assert.assertEquals(2, sqlUpdate.getUpdateColumns().size());
        Assert.assertFalse(sqlUpdate.getUpdateValues().isEmpty());
        Assert.assertEquals(2, sqlUpdate.getUpdateValues().size());
        Assert.assertNotNull(sqlUpdate.getWhereClause());
        Assert.assertTrue(sqlUpdate.getSourceSql().contains("'abc', 12, abs ( - 1 ), 'a', 'b', 12, 14, 17"));
        System.out.println(sqlUpdate.getSourceSql());

        sql = "update table1 a, table2 b set a.name=b.name, a.age=b.age where a.id=b.id and a.name not like 'xx'";
        sqlUpdate = sqlParser.parseUpdate(sql);
        Assert.assertNotNull(sqlUpdate.getTable());
        Assert.assertNotNull(sqlUpdate.getJoinTable());
        Assert.assertFalse(sqlUpdate.getUpdateColumns().isEmpty());
        Assert.assertEquals(2, sqlUpdate.getUpdateColumns().size());
        Assert.assertFalse(sqlUpdate.getUpdateValues().isEmpty());
        Assert.assertEquals(2, sqlUpdate.getUpdateValues().size());
        Assert.assertNotNull(sqlUpdate.getWhereClause());
        Assert.assertTrue(sqlUpdate.getSourceSql().contains("b.name, b.age, b.id, 'xx'"));
        System.out.println(sqlUpdate.getSourceSql());

        sql = "update table1 a, (select /*+ mapjoin(d) */ /*+ cache()*/ CASE \n" +
                "        WHEN salary > 100000 THEN 'High Salary'\n" +
                "        WHEN salary BETWEEN 50000 AND 100000 THEN 'Medium Salary'\n" +
                "        ELSE 'Low Salary'\n" +
                "    END AS salary_level from table2 c left join table3 d on c.id=d.id) b set a.name=b.name,a.age=b.age where a.id=b.id and a.name not like 'xx'";
        sqlUpdate = sqlParser.parseUpdate(sql);
        Assert.assertNotNull(sqlUpdate.getTable());
        Assert.assertNotNull(sqlUpdate.getJoinSql());
        Assert.assertEquals(2, sqlUpdate.getUpdateColumns().size());
        Assert.assertFalse(sqlUpdate.getUpdateValues().isEmpty());
        Assert.assertEquals(2, sqlUpdate.getUpdateValues().size());
        Assert.assertNotNull(sqlUpdate.getWhereClause());
        Assert.assertEquals(2, sqlUpdate.getHints().size());
        Assert.assertEquals("mapjoin", sqlUpdate.getHints().get(0).getHintName());
        Assert.assertFalse(sqlUpdate.getHints().get(0).getArguments().isEmpty());
        Assert.assertEquals("d", sqlUpdate.getHints().get(0).getArguments().get(0));
        System.out.println(sqlUpdate.getSourceSql());
    }

    @Test
    public void testDelete() {
        String sql = "delete from table1 where id=1 and (name='abc' or age in (12,14,17))";
        SQLDelete sqlDelete = sqlParser.parseDelete(sql);
        Assert.assertNotNull(sqlDelete.getTable());
        Assert.assertNotNull(sqlDelete.getWhereClause());
        Assert.assertTrue(sqlDelete.getSourceSql().contains("1, 'abc', 12, 14, 17"));
        System.out.println(sqlDelete.getSourceSql());

        sql = "delete from table1 a, table2 b where a.id=b.id and a.name not like 'xx'";
        sqlDelete = sqlParser.parseDelete(sql);
        Assert.assertNotNull(sqlDelete.getTable());
        Assert.assertNotNull(sqlDelete.getJoinTable());
        Assert.assertNotNull(sqlDelete.getWhereClause());
        Assert.assertTrue(sqlDelete.getSourceSql().contains("b.id, 'xx'"));
        System.out.println(sqlDelete.getSourceSql());

        sql = "delete from table1 a, (select /*+ mapjoin (d) */ /*+ cache() */ * from table2 c left join table3 d on c.id=d.id where age>10) b where a.id=b.id and a.name not like 'xx'";
        sqlDelete = sqlParser.parseDelete(sql);
        Assert.assertNotNull(sqlDelete.getTable());
        Assert.assertNotNull(sqlDelete.getJoinSql());
        Assert.assertEquals("mapjoin", sqlDelete.getHints().get(0).getHintName());
        Assert.assertFalse(sqlDelete.getHints().get(0).getArguments().isEmpty());
        Assert.assertEquals("d", sqlDelete.getHints().get(0).getArguments().get(0));
        System.out.println(sqlDelete.getSourceSql());
    }

    @Test
    public void testSelect() {
        String sql = "select * from table1 where id=1";
        SQLSelect sqlSelect = sqlParser.parseSelect(sql);
        Assert.assertNotNull(sqlSelect.getSourceSql());

        sql = "select /*+ mapjoin(b) */ /*+ cache() */ a.id, a.name, b.role_name from table1 a left join role b on a.role_id=b.id where a.id>0";
        sqlSelect = sqlParser.parseSelect(sql);
        Assert.assertNotNull(sqlSelect.getSourceSql());
        Assert.assertEquals(2, sqlSelect.getHints().size());
        Assert.assertEquals("mapjoin", sqlSelect.getHints().get(0).getHintName());
        Assert.assertFalse(sqlSelect.getHints().get(0).getArguments().isEmpty());
        Assert.assertEquals("b", sqlSelect.getHints().get(0).getArguments().get(0));
        System.out.println(sqlSelect.getSourceSql());


        sql = "select /*+ show() */ count(1) into \"${param}\" from table1";
        sqlSelect = sqlParser.parseSelect(sql);
        Assert.assertNotNull(sqlSelect.getSourceSql());
        Assert.assertNotNull(sqlSelect.getIntoParams());
        Assert.assertEquals(1, sqlSelect.getHints().size());
        Assert.assertEquals("show", sqlSelect.getHints().get(0).getHintName());
        Assert.assertFalse(sqlSelect.getSourceSql().contains(" into "));
        System.out.println(sqlSelect.getSourceSql());
    }

    @Test
    public void testMerge() {
        String sql = "merge into table1 a using table2 b on a.id=b.id and a.name=b.name " +
                "when matched then update set a.name = b.name, a.age = b.age where a.id<>-1 " +
                "when not matched then insert (id, name, age) values (b.id, b.name, b.age)";
        SQLMerge sqlMerge = sqlParser.parseMerge(sql);
        Assert.assertNotNull(sqlMerge.getTable());
        Assert.assertNotNull(sqlMerge.getUsingTable());
        Assert.assertNotNull(sqlMerge.getOnSelectItems());
        Assert.assertNotNull(sqlMerge.getSqlInsert());
        Assert.assertNotNull(sqlMerge.getSqlUpdate());
        Assert.assertNotNull(sqlMerge.getAllWhereClause());
    }
}
