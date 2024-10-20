//package com.github.ares.sql.test;
//
//import com.github.ares.api.common.EngineType;
//import com.github.ares.api.common.EngineTypeVersion;
//import com.github.ares.api.common.ExecutionEngineType;
//import com.github.ares.sql.expression.sql.ExpressionEngine;
//import org.junit.Test;
//
//public class SimpleSqlEnginTest {
//    @Test
//    public void test01() {
//        ExecutionEngineType.init(EngineType.SPARK, EngineTypeVersion.SPARK3);
//        String sql = "select 'xvv_'||cast(power(3,2) as int)";
//        sql = "select add_months('2021-01-01 12', 1)";
//        sql = "select extract(YEAR FROM '2019-08-12 01:00:00.123456')";
//        sql = "select ilike('abc','a')";
//        sql = "select 1%2.1";
////        sql = "select rlike('abc','a')";
//        ExpressionEngine sqlEngine = new ExpressionEngine();
//        sqlEngine.init();
////        Assert.assertFalse(sqlEngine.computeForBool());
//        Object val = sqlEngine.evaluate(sql);
//        val = val;
//    }
//}
