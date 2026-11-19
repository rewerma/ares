package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.api.table.type.BasicType;
import org.junit.Assert;
import org.junit.Test;

public class Gbase8sDialectTest {

    @Test
    public void quotesAndUsesFirst() {
        Assert.assertEquals(
                "\"stores\".\"customer\"", new Gbase8sDialect().quoteIdentifier("stores.customer"));
        Assert.assertTrue(
                new Gbase8sDialectFactory()
                        .acceptsURL(
                                "jdbc:gbasedbt-sqli://10.0.0.1:9088/testdb:GBASEDBTSERVER=gbase01"));
        String sql = Gbase8sDialect.nextChunkSql("\"id\"", "\"stores\".\"customer\"", 50);
        Assert.assertTrue(sql.contains("SELECT FIRST 50 \"id\""));
        Assert.assertFalse(sql.contains("LIMIT"));
    }

    @Test
    public void mapsSerialToInt() {
        Assert.assertEquals(
                BasicType.INT_TYPE,
                Gbase8sTypeConverter.INSTANCE
                        .convert(BasicTypeDefine.builder().name("id").dataType("SERIAL").build())
                        .getDataType());
    }
}
