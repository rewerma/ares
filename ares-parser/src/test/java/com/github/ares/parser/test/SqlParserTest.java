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
import com.github.ares.parser.sqlparser.sparksql.SelectSqlParser;
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
        String sql =
                "insert into table1 (id, name, age) values (1, 'abc', 12), (2, 'def', 14), (3, 'ghi', 17)";
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

        sql =
                "insert into table1 select a.id, a.name, b.role_name from table1 a left join role b on a.role_id=b.id where a.id>0";
        sqlInsert = sqlParser.parseInsert(sql);
        Assert.assertNotNull(sqlInsert.getTable());
        Assert.assertTrue(sqlInsert.getColumns().isEmpty());
        Assert.assertNull(sqlInsert.getValuesArray());
        Assert.assertNotNull(sqlInsert.getSourceSql());
        System.out.println(sqlInsert.getSourceSql());

        sql =
                "insert into table1 (id, name, role_name) select /*+ mapjoin(b) */ /*+ cache()*/ a.id, a.name, b.role_name "
                        + "from table1 a left join role b on a.role_id=b.id where a.id>0";
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
        String sql =
                "update table1 a set a.name='abc',a.age=12 where a.id<>abs(-1) and (name='a' or name like 'b') and a.age in (12,14,17)";
        SQLUpdate sqlUpdate = sqlParser.parseUpdate(sql);
        Assert.assertNotNull(sqlUpdate.getTable());
        Assert.assertFalse(sqlUpdate.getUpdateColumns().isEmpty());
        Assert.assertEquals(2, sqlUpdate.getUpdateColumns().size());
        Assert.assertFalse(sqlUpdate.getUpdateValues().isEmpty());
        Assert.assertEquals(2, sqlUpdate.getUpdateValues().size());
        Assert.assertNotNull(sqlUpdate.getWhereClause());
        Assert.assertTrue(
                sqlUpdate.getSourceSql().contains("'abc', 12, abs ( - 1 ), 'a', 'b', 12, 14, 17"));
        System.out.println(sqlUpdate.getSourceSql());

        sql =
                "update table1 a, table2 b set a.name=b.name, a.age=b.age where a.id=b.id and a.name not like 'xx'";
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

        sql =
                "update table1 a, (select /*+ mapjoin(d) */ /*+ cache()*/ CASE \n"
                        + "        WHEN salary > 100000 THEN 'High Salary'\n"
                        + "        WHEN salary BETWEEN 50000 AND 100000 THEN 'Medium Salary'\n"
                        + "        ELSE 'Low Salary'\n"
                        + "    END AS salary_level from table2 c left join table3 d on c.id=d.id) b set a.name=b.name,a.age=b.age where a.id=b.id and a.name not like 'xx'";
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

        sql =
                "delete from table1 a, (select /*+ mapjoin (d) */ /*+ cache() */ * from table2 c left join table3 d on c.id=d.id where age>10) b where a.id=b.id and a.name not like 'xx'";
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

        sql =
                "select /*+ mapjoin(b) */ /*+ cache() */ a.id, a.name, b.role_name from table1 a left join role b on a.role_id=b.id where a.id>0";
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

        sql = "select * from table1 limit 10";
        sqlSelect = sqlParser.parseSelect(sql);
        Assert.assertTrue(normalizeSql(sqlSelect.getSourceSql()).contains("limit 10"));
        Assert.assertTrue(SelectSqlParser.hasOuterLimit(sqlSelect.getSourceSql()));

        sql = "select * from table1 order by id desc limit 10";
        sqlSelect = sqlParser.parseSelect(sql);
        String normalized = normalizeSql(sqlSelect.getSourceSql());
        Assert.assertTrue(normalized.contains("order by id desc"));
        Assert.assertTrue(normalized.contains("limit 10"));
        Assert.assertTrue(SelectSqlParser.hasOuterLimit(sqlSelect.getSourceSql()));

        Assert.assertFalse(SelectSqlParser.hasOuterLimit("select * from table1"));
        Assert.assertFalse(
                SelectSqlParser.hasOuterLimit("select * from (select * from table1 limit 5) a"));
        Assert.assertTrue(SelectSqlParser.hasOuterLimit("select * from table1 LIMIT ALL"));
    }

    @Test
    public void testCte() {
        String sql =
                "with adult as (select id, name from table1 where age >= 18), "
                        + "names as (select name from adult) "
                        + "select /*+ cache() */ name from names where name is not null "
                        + "order by name limit 5";
        SQLSelect sqlSelect = sqlParser.parseSelect(sql);
        String normalized = normalizeSql(sqlSelect.getSourceSql());
        Assert.assertTrue(normalized.startsWith("with adult as"));
        Assert.assertTrue(normalized.contains("names as"));
        Assert.assertTrue(normalized.contains("from names"));
        Assert.assertTrue(normalized.contains("order by name"));
        Assert.assertTrue(normalized.contains("limit 5"));
        Assert.assertFalse(normalized.contains("cache"));
        Assert.assertEquals(1, sqlSelect.getHints().size());
        Assert.assertEquals("cache", sqlSelect.getHints().get(0).getHintName());
        Assert.assertTrue(SelectSqlParser.hasOuterLimit(sqlSelect.getSourceSql()));

        sql =
                "with adult (id, name) as (select id, name from table1) "
                        + "select name into \"${v_name}\" from adult";
        sqlSelect = sqlParser.parseSelect(sql);
        normalized = normalizeSql(sqlSelect.getSourceSql());
        Assert.assertTrue(normalized.contains("with adult ( id , name ) as"));
        Assert.assertFalse(normalized.contains(" into "));
        Assert.assertEquals("v_name", sqlSelect.getIntoParams().get(0));

        sql =
                "insert into table1 with cte as (select a.id, a.name from table1 a) "
                        + "select /*+ show() */ id, name from cte order by id limit 10";
        SQLInsert sqlInsert = sqlParser.parseInsert(sql);
        normalized = normalizeSql(sqlInsert.getSourceSql());
        Assert.assertTrue(normalized.startsWith("with cte as"));
        Assert.assertTrue(normalized.contains("from cte"));
        Assert.assertTrue(normalized.contains("order by id"));
        Assert.assertTrue(normalized.contains("limit 10"));
        Assert.assertEquals("show", sqlInsert.getHints().get(0).getHintName());

        sql =
                "with outer_cte as (select id, name from table1) "
                        + "insert into table1 "
                        + "with inner_cte as (select id, name from outer_cte) "
                        + "select id, name from inner_cte";
        sqlInsert = sqlParser.parseInsert(sql);
        normalized = normalizeSql(sqlInsert.getSourceSql());
        Assert.assertTrue(normalized.startsWith("with outer_cte as"));
        Assert.assertTrue(normalized.contains("with inner_cte as"));
        Assert.assertTrue(normalized.contains("from inner_cte"));
        Assert.assertTrue(normalized.contains("__ares_cte"));

        sql =
                "with cte as (select id, name from table2) "
                        + "update table1 a, cte b set a.name=b.name where a.id=b.id";
        SQLUpdate sqlUpdate = sqlParser.parseUpdate(sql);
        normalized = normalizeSql(sqlUpdate.getSourceSql());
        Assert.assertTrue(normalized.startsWith("with cte as"));
        Assert.assertTrue(normalized.contains("from cte b"));
        Assert.assertTrue(normalizeSql(sqlUpdate.getLeadingCte()).startsWith("with cte as"));

        sql =
                "update table1 a, (with cte as (select id, name from table2) "
                        + "select id, name from cte) b set a.name=b.name where a.id=b.id";
        sqlUpdate = sqlParser.parseUpdate(sql);
        Assert.assertTrue(normalizeSql(sqlUpdate.getJoinSql()).startsWith("with cte as"));
        Assert.assertNull(sqlUpdate.getLeadingCte());

        sql =
                "with cte as (select id from table2) "
                        + "delete from table1 a, cte b where a.id=b.id";
        SQLDelete sqlDelete = sqlParser.parseDelete(sql);
        normalized = normalizeSql(sqlDelete.getSourceSql());
        Assert.assertTrue(normalized.startsWith("with cte as"));
        Assert.assertTrue(normalized.contains("from cte b"));

        sql =
                "delete from table1 a, (with cte as (select id from table2) select id from cte) b "
                        + "where a.id=b.id";
        sqlDelete = sqlParser.parseDelete(sql);
        Assert.assertTrue(normalizeSql(sqlDelete.getJoinSql()).startsWith("with cte as"));

        sql =
                "with src as (select id, name, age from table2) "
                        + "merge into table1 a using src b on a.id=b.id "
                        + "when matched then update set a.name = b.name "
                        + "when not matched then insert (id, name) values (b.id, b.name)";
        SQLMerge sqlMerge = sqlParser.parseMerge(sql);
        Assert.assertEquals("src", sqlMerge.getUsingTable());
        Assert.assertTrue(normalizeSql(sqlMerge.getLeadingCte()).startsWith("with src as"));
        Assert.assertTrue(
                normalizeSql(sqlMerge.getSqlUpdate().getSourceSql()).startsWith("with src as"));
        Assert.assertTrue(
                normalizeSql(sqlMerge.getSqlInsert().getSourceSql()).startsWith("with src as"));

        sql =
                "merge into table1 a using (with src as (select id, name from table2) "
                        + "select id, name from src) b on a.id=b.id "
                        + "when matched then update set a.name = b.name";
        sqlMerge = sqlParser.parseMerge(sql);
        Assert.assertTrue(normalizeSql(sqlMerge.getUsingSql()).startsWith("with src as"));
        Assert.assertNull(sqlMerge.getLeadingCte());
    }

    private static String normalizeSql(String sql) {
        return sql.replaceAll("\\s+", " ").trim().toLowerCase();
    }

    @Test
    public void testMerge() {
        String sql =
                "merge into table1 a using table2 b on a.id=b.id and a.name=b.name "
                        + "when matched then update set a.name = b.name, a.age = b.age where a.id<>-1 "
                        + "when not matched then insert (id, name, age) values (b.id, b.name, b.age)";
        SQLMerge sqlMerge = sqlParser.parseMerge(sql);
        Assert.assertNotNull(sqlMerge.getTable());
        Assert.assertNotNull(sqlMerge.getUsingTable());
        Assert.assertNotNull(sqlMerge.getOnSelectItems());
        Assert.assertNotNull(sqlMerge.getSqlInsert());
        Assert.assertNotNull(sqlMerge.getSqlUpdate());
        Assert.assertNotNull(sqlMerge.getAllWhereClause());
    }
}
